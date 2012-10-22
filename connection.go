package kafka

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
)

type Conn struct {
	conn          net.Conn
	rw            *bufio.ReadWriter
	responseQueue chan pendingResponse
}

// Can return either an error or a message
type MessageResponse struct {
	Message Message
	Err     error
}

type fetchResponseChan chan *MessageResponse
type FetchResponseChan <-chan *MessageResponse

type OffsetsResponse struct {
	Offsets []Offset
	Err     error
}

type offsetsResponseChan chan *OffsetsResponse
type OffsetsResponseChan <-chan *OffsetsResponse

type responseType int8

// This will only have one type of chan in it
// acts as a union
type pendingResponse struct {
	fetch   fetchResponseChan
	offsets offsetsResponseChan
}

const defaultQueueSize = 128

func Dial(addr string) (c *Conn, err error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	respQueue := make(chan pendingResponse, defaultQueueSize)

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	c = &Conn{
		conn:          conn,
		rw:            rw,
		responseQueue: respQueue,
	}

	go c.readWorker()

	return c, nil
}

// We are going to reuse the buffers for fetch and multifetch, so don't keep the slices around
// This will yield one per message and close when it's done
func (c *Conn) MultiFetch(req MultiFetchRequest) (results FetchResponseChan, err error) {
	resp := make(fetchResponseChan)
	c.responseQueue <- pendingResponse{fetch: resp}

	if _, err = c.writeRequest(req); err != nil {
		return nil, err
	}

	results = (<-chan *MessageResponse)(resp)
	return
}

// We are going to reuse the buffers for fetch and multifetch, so don't keep the slices around
func (c *Conn) Fetch(req *FetchRequest) (results FetchResponseChan, err error) {
	resp := make(fetchResponseChan)
	c.responseQueue <- pendingResponse{fetch: resp}

	if _, err = c.writeRequest(req); err != nil {
		return nil, err
	}

	results = (<-chan *MessageResponse)(resp)
	return
}

// We are going to reuse the buffers for fetch and multifetch, so don't keep the slices around
func (c *Conn) Offsets(req *OffsetsRequest) (results OffsetsResponseChan, err error) {
	resp := make(offsetsResponseChan)
	c.responseQueue <- pendingResponse{offsets: resp}

	if _, err = c.writeRequest(req); err != nil {
		return nil, err
	}

	results = (<-chan *OffsetsResponse)(resp)
	return
}

func (c *Conn) MultiProduce(req MultiProduceRequest) (err error) {
	_, err = c.writeRequest(req)
	return
}

func (c *Conn) Produce(req *ProduceRequest) (err error) {
	_, err = c.writeRequest(req)
	return
}

func (c *Conn) writeRequest(req request) (n int64, err error) {
	totalLen := int32(req.Len() + 2) // req, type

	if n, err = binwrite(c.rw, totalLen, req.Type()); err != nil {
		return -1, err
	}

	var nn int64
	if nn, err = req.WriteTo(c.rw); err != nil {
		return -1, err
	}
	n += nn

	if n != int64(totalLen)+4 {
		log.Panicln("Did not compute length properly. expected to write", totalLen+4, "but wrote", n)
	}
	err = c.rw.Flush()
	return
}

func (c *Conn) failResponses(err error) {
	for rc := range c.responseQueue {
		switch {
		case rc.offsets != nil:
			rc.offsets <- &OffsetsResponse{Err: err}
			close(rc.offsets)
		case rc.fetch != nil:
			rc.fetch <- &MessageResponse{Err: err}
			close(rc.fetch)
		}
	}
	close(c.responseQueue)
}

// TODO handle errors better
// TODO refactor to make smaller
func (c *Conn) readWorker() {
	defer log.Println("Read worker finishing")

	message, lastMessage := Message{}, Message{}

	for {
		var responseLength int32
		var code ErrorCode

		if err := binread(c.rw, &responseLength); err != nil {
			c.failResponses(err)
			return
		}

		remainingResponse := io.LimitReader(c.rw, int64(responseLength))

		if err := binread(remainingResponse, &code); err != nil {
			c.failResponses(err)
			return
		}

		var rc pendingResponse
		select {
		case rc = <-c.responseQueue:
		default:
			log.Panicln("We received bytes without expecting them")

		}

		failResponse := func(err error) {
			switch {
			case rc.fetch != nil:
				rc.fetch <- &MessageResponse{Err: err}
				close(rc.fetch)
			}
		}

		// if the fetch request we sent has an error code, we only fail this one channel
		if code != ErrorCodeNoError {
			failResponse(code)
			continue
		}

		switch {
		case rc.offsets != nil:
			log.Println("got offset response")
			var numOffsets int32
			if err := binread(remainingResponse, &numOffsets); err != nil {
				failResponse(err)
				c.failResponses(err)
				return
			}

			offsets := make([]Offset, int(numOffsets))
			for i := range offsets {
				log.Println("Reading offset")
				if err := binread(remainingResponse, &offsets[i]); err != nil {
					failResponse(err)
					c.failResponses(err)
					return
				}
			}

			rc.offsets <- &OffsetsResponse{Offsets: offsets}
			close(rc.offsets)

		case rc.fetch != nil:

			var compression CompressionType
			var length int32
			var checksum uint32
			var magic MagicType

		doneReadFetch:

			for {
				switch err := binread(remainingResponse, &length); err {
				case nil:
				case io.EOF:
					break doneReadFetch
				default:
					failResponse(err)
					c.failResponses(err)
					return
				}

				payloadLen := length - messageHeaderSize

				if cap(message) < int(payloadLen) {
					// Let's keep the bigger of the two messages
					if cap(message) > cap(lastMessage) {
						lastMessage = message
					}
					message = make(Message, payloadLen)
				} else {
					message = message[0:payloadLen]
				}

				if err := binread(remainingResponse, &magic, &compression, &checksum, message); err != nil {
					failResponse(err)
					c.failResponses(err)
					return
				}

				if compression != CompressionTypeNone {
					err := fmt.Errorf("Only support none compression")
					failResponse(err)
					c.failResponses(err)
					return
				}

				if magic != MagicTypeWithCompression {
					err := fmt.Errorf("Only support new message format (with magic type of 1)")
					failResponse(err)
					c.failResponses(err)
					return
				}

				if crc32.ChecksumIEEE(message) != checksum {
					err := fmt.Errorf("Got invalid checksum (msg: %s)", string(message), crc32.ChecksumIEEE(message), checksum)
					failResponse(err)
					c.failResponses(err)
				}

				// If we made it here, we have a valid message
				rc.fetch <- &MessageResponse{Message: message}

				// Swap our buffers.  We can only guarantee our
				// last one is not in use after the newest one
				// is consumed
				tmp := lastMessage
				lastMessage = message
				message = tmp
			}

			close(rc.fetch)
		}
		if remainingResponse.(*io.LimitedReader).N != 0 {
			log.Panicln(
				"Did not read eniter message. we have",
				remainingResponse.(*io.LimitedReader).N,
				"remaining bytes")
		}
	}
}

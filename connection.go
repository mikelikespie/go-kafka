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
	responseQueue chan responseJob

	// Buffers
	message, lastMessage Message
}

// Can return either an error or a message
// Offset is the offset *after* this message
type FetchResponse struct {
	Message Message
	TopicPartitionOffset

	Err error
}

type FetchResponseChan chan FetchResponse


type OffsetsResponse struct {
	// Add the topic and partition with it to make things easier
	Offsets []TopicPartitionOffset
	Err     error
}

type OffsetsResponseChan chan OffsetsResponse

type responseType int8

type responseJob interface {
	Close()
	Fail(err error)
	ReadResponse(r io.Reader, c *Conn) (err error)
}

type offsetsResponseJob struct {
	TopicPartition
	ch chan OffsetsResponse
}

func (j *offsetsResponseJob) Close() {
	close(j.ch)
}

func (j *offsetsResponseJob) Fail(err error) {
	j.ch <- OffsetsResponse{Err: err}
	close(j.ch)
}

func (j *offsetsResponseJob) ReadResponse(r io.Reader, c *Conn) (err error) {
	log.Println("got offset response")
	var numOffsets int32
	if err = binread(r, &numOffsets); err != nil {
		return
	}

	offsets := make([]TopicPartitionOffset, int(numOffsets))
	for i := range offsets {
		log.Println("Reading offset")
		if err = binread(r, &offsets[i].Offset); err != nil {
			return
		}

		offsets[i].TopicPartition = j.TopicPartition
	}

	j.ch <- OffsetsResponse{Offsets: offsets}

	return
}

type fetchResponseJob struct {
	TopicPartitionOffset
	ch FetchResponseChan
}

func (j *fetchResponseJob) Close() {
	close(j.ch)
}

func (j *fetchResponseJob) Fail(err error) {
	j.ch <- FetchResponse{Err: err}
	close(j.ch)
}

func (j *fetchResponseJob) ReadResponse(r io.Reader, c *Conn) (err error) {
	return c.readMessagesSet(j.TopicPartitionOffset, j.ch, r)
}

type multiFetchResponseJob struct {
	mfr MultiFetchRequest
	ch  FetchResponseChan
}

func (j *multiFetchResponseJob) Close() {
	close(j.ch)
}

func (j *multiFetchResponseJob) Fail(err error) {
	j.ch <- FetchResponse{Err: err}
	close(j.ch)
}

func (j *multiFetchResponseJob) ReadResponse(r io.Reader, c *Conn) (err error) {
	for _, info := range j.mfr {
		var messageSetLen int32
		switch err = binread(r, &messageSetLen); err {
		case nil:
		case io.EOF:
			return nil
		default:
			return
		}

		messageSetReader := io.LimitReader(r, int64(messageSetLen))
		var code ErrorCode
		if err = binread(messageSetReader, &code); err != nil {
			return
		}

		if code != ErrorCodeNoError {
			return code
		}

		err = c.readMessagesSet(info.TopicPartitionOffset, j.ch, messageSetReader)
	}
	return
}

const defaultQueueSize = 128

func Dial(addr string) (c *Conn, err error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	respQueue := make(chan responseJob, defaultQueueSize)

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	c = &Conn{
		conn:          conn,
		rw:            rw,
		responseQueue: respQueue,
		message:       []byte{},
		lastMessage:   []byte{},
	}

	go c.readWorker()

	return c, nil
}

// We are going to reuse the buffers for fetch and multifetch, so don't keep the slices around
// This will yield one per message and close when it's done
func (c *Conn) MultiFetch(req MultiFetchRequest) (results FetchResponseChan, err error) {

	resp := make(FetchResponseChan)
	c.responseQueue <- &multiFetchResponseJob{
		ch: resp,
		mfr: req,
	}

	if _, err = c.writeRequest(req); err != nil {
		return nil, err
	}

	results = resp
	return
}

// We are going to reuse the buffers for fetch and multifetch, so don't keep the slices around
func (c *Conn) Fetch(req *FetchRequest) (results FetchResponseChan, err error) {
	resp := make(FetchResponseChan)

	c.responseQueue <- &fetchResponseJob{
		ch:                   resp,
		TopicPartitionOffset: req.TopicPartitionOffset,
	}

	if _, err = c.writeRequest(req); err != nil {
		return nil, err
	}

	results = resp
	return
}

// We are going to reuse the buffers for fetch and multifetch, so don't keep the slices around
func (c *Conn) Offsets(req *OffsetsRequest) (results OffsetsResponseChan, err error) {
	resp := make(OffsetsResponseChan)
	c.responseQueue <- &offsetsResponseJob{
		TopicPartition: req.TopicPartition,
		ch:             resp,
	}

	if _, err = c.writeRequest(req); err != nil {
		return nil, err
	}

	results = resp
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

// This will increment rc's offset
func (c *Conn) readMessagesSet(info TopicPartitionOffset, ch FetchResponseChan, messageStream io.Reader) (err error) {
	var compression CompressionType
	var length int32
	var checksum uint32
	var magic MagicType

	for {
		switch err = binread(messageStream, &length); err {
		case nil:
		case io.EOF:
			return nil
		default:
			return
		}

		payloadLen := length - messageHeaderSize

		mCap := cap(c.message)
		if mCap < int(payloadLen) {
			// Let's keep the bigger of the two messages
			if mCap > cap(c.lastMessage) {
				c.lastMessage = c.message
			}
			c.message = make(Message, payloadLen)
		} else {
			c.message = c.message[:payloadLen]
		}

		if err = binread(messageStream, &magic, &compression, &checksum, c.message); err != nil {
			return
		}

		if compression != CompressionTypeNone {
			return fmt.Errorf("Only support none compression")
		}

		if magic != MagicTypeWithCompression {
			return fmt.Errorf("Only support new message format (with magic type of 1)")
		}

		if crc32.ChecksumIEEE(c.message) != checksum {
			return fmt.Errorf("Got invalid checksum")
		}

		info.Offset += Offset(length + 4)

		// If we made it here, we have a valid message
		ch <- FetchResponse{
			Message:              c.message,
			TopicPartitionOffset: info,
		}

		// Swap our buffers.  We can only guarantee our last one is not
		// in use after the newest one is consumed
		tmp := c.lastMessage
		c.lastMessage = c.message
		c.message = tmp
	}
	return
}

// reads stream and processes puts the response into pr's channel
func (c *Conn) failResponses(err error) {
	for rc := range c.responseQueue {
		rc.Fail(err)
	}
}

func (c *Conn) doRead() (err error) {
	var responseLength int32
	var code ErrorCode

	if err = binread(c.rw, &responseLength); err != nil {
		return
	}

	remainingResponse := io.LimitReader(c.rw, int64(responseLength))

	if err = binread(remainingResponse, &code); err != nil {
		return
	}

	// if the fetch request we sent has an error code, we only fail this one channel
	if code != ErrorCodeNoError {
		return code
	}

	var j responseJob

	// Just to check for error states
	// We should never get response without knowing it
	// Also if we're idle, we want to read in case the stream got closed
	select {
	case j = <-c.responseQueue:
	default:
		log.Panicln("We received bytes without expecting them")
	}

	if err = j.ReadResponse(remainingResponse, c); err != nil {
		j.Fail(err)
		return
	} else {
		j.Close()
	}
	// Sanity check to make sure we implemented the protocol correctly
	if remainingResponse.(*io.LimitedReader).N != 0 {
		log.Panicln(
			"Did not read eniter message. we have",
			remainingResponse.(*io.LimitedReader).N,
			"remaining bytes")
	}
	return

}

func (c *Conn) readWorker() {
	defer log.Println("Read worker finishing")

	for {
		err := c.doRead()
		if err != nil {
			log.Println("Connection closed with error:", err)
			return
		}
	}
}

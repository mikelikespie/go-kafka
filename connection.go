package kafka

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
)

type SimpleConsumer struct {
	conn          net.Conn
	rw            *bufio.ReadWriter
	responseQueue chan responseJob
}

const defaultQueueSize = 128

func Dial(addr string) (c *SimpleConsumer, err error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	respQueue := make(chan responseJob, defaultQueueSize)

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	c = &SimpleConsumer{
		conn:          conn,
		rw:            rw,
		responseQueue: respQueue,
	}

	go c.readWorker()

	return c, nil
}

// We are going to reuse the buffers for fetch and multifetch, so don't keep the slices around
// This will yield one per message and close when it's done
func (c *SimpleConsumer) MultiFetch(req MultiFetchRequest) (results FetchResponseChan, err error) {

	resp := make(FetchResponseChan)
	c.responseQueue <- &multiFetchResponseJob{
		ch:  resp,
		mfr: req,
	}

	if _, err = c.writeRequest(req); err != nil {
		panic(err)
		return nil, err
	}

	results = resp
	return
}

// We are going to reuse the buffers for fetch and multifetch, so don't keep the slices around
func (c *SimpleConsumer) Fetch(req FetchRequest) (results FetchResponseChan, err error) {
	resp := make(FetchResponseChan)

	c.responseQueue <- &fetchResponseJob{
		ch:                   resp,
		TopicPartitionOffset: req.TopicPartitionOffset,
	}

	if _, err = c.writeRequest(&req); err != nil {
		return nil, err
	}

	results = resp
	return
}

// We are going to reuse the buffers for fetch and multifetch, so don't keep the slices around
func (c *SimpleConsumer) Offsets(req OffsetsRequest) (results OffsetsResponseChan, err error) {
	resp := make(OffsetsResponseChan)
	c.responseQueue <- &offsetsResponseJob{
		TopicPartition: req.TopicPartition,
		ch:             resp,
	}

	if _, err = c.writeRequest(&req); err != nil {
		return nil, err
	}

	results = resp
	return
}

func (c *SimpleConsumer) MultiProduce(req MultiProduceRequest) (err error) {
	_, err = c.writeRequest(req)
	return
}

func (c *SimpleConsumer) Produce(req *ProduceRequest) (err error) {
	_, err = c.writeRequest(req)
	return
}

func (c *SimpleConsumer) writeRequest(req request) (n int64, err error) {
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
func (c *SimpleConsumer) readMessagesSet(info TopicPartitionOffset, ch FetchResponseChan, messageStream io.Reader) (err error) {
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

		// TODO: reuse these.  it's not that hard
		message := make(Message, payloadLen)

		switch err = binread(messageStream, &magic, &compression, &checksum, message); {
		case err != nil:
			return err
		case compression != CompressionTypeNone:
			return fmt.Errorf("Only support none compression")
		case magic != MagicTypeWithCompression:
			return fmt.Errorf("Only support new message format (with magic type of 1)")
		case crc32.ChecksumIEEE(message) != checksum:
			return fmt.Errorf("Got invalid checksum")
		}

		info.Offset += Offset(length + 4)

		// If we made it here, we have a valid message
		ch <- FetchResponse{
			Message:              message,
			TopicPartitionOffset: info,
		}
	}

	return
}

// reads stream and processes puts the response into pr's channel
func (c *SimpleConsumer) failResponses(err error) {
	for rc := range c.responseQueue {
		rc.Fail(err)
	}
}

func (c *SimpleConsumer) doRead() (err error) {
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

func (c *SimpleConsumer) readWorker() {
	defer log.Println("Read worker finishing")

	for {
		err := c.doRead()
		if err != nil {
			log.Println("Connection closed with error:", err)
			return
		}
	}
}

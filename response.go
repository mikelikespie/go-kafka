package kafka

import (
	"io"
	"log"
)

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
	ReadResponse(r io.Reader, c *SimpleConsumer) (err error)
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

func (j *offsetsResponseJob) ReadResponse(r io.Reader, c *SimpleConsumer) (err error) {
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

func (j *fetchResponseJob) ReadResponse(r io.Reader, c *SimpleConsumer) (err error) {
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

func (j *multiFetchResponseJob) ReadResponse(r io.Reader, c *SimpleConsumer) (err error) {
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
		switch err = binread(messageSetReader, &code); {
		case err != nil :
			return err
		case code != ErrorCodeNoError:
			return code
		}

		err = c.readMessagesSet(info.TopicPartitionOffset, j.ch, messageSetReader)
	}
	return
}


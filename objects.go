package kafka

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

type requestType int16

const (
	requestTypeProduce      requestType = 0
	requestTypeFetch        requestType = 1
	requestTypeMultiFetch   requestType = 2
	requestTypeMultiProduce requestType = 3
	requestTypeOffsets      requestType = 4
)

// We'll just use the codes as errors
type ErrorCode int16

const (
	ErrorCodeUnknown          ErrorCode = -1
	ErrorCodeNoError          ErrorCode = 0
	ErrorCodeOffsetOutOfRange ErrorCode = 1
	ErrorCodeInvalidMessage   ErrorCode = 2
	ErrorCodeWrongPartition   ErrorCode = 3
	ErrorCodeInvalidFetchSize ErrorCode = 4
)

var errorMessages = map[ErrorCode]string{
	ErrorCodeUnknown:          "Unknown Error",
	ErrorCodeNoError:          "Success",
	ErrorCodeOffsetOutOfRange: "Offset requested is no longer available on the server",
	ErrorCodeInvalidMessage:   "A message you sent failed its checksum and is corrupt.",
	ErrorCodeWrongPartition:   "You tried to access a partition that doesn't exist (was not between 0 and (num_partitions - 1)).",
	ErrorCodeInvalidFetchSize: "The size you requested for fetching is smaller than the message you're trying to fetch.",
}

func (ec ErrorCode) Error() string {
	return errorMessages[ec]
}

type Offset int64
type Partition int32

type MagicType int8

const (
	MagicTypeWithoutCompression MagicType = 0
	MagicTypeWithCompression    MagicType = 1
)

type CompressionType int8

const (
	CompressionTypeNone   CompressionType = 0
	CompressionTypeGZip   CompressionType = 1
	CompressionTypeSnappy CompressionType = 2
)

type request interface {
	io.WriterTo

	Len() int32
	Type() requestType
}

type OffsetTime int64

const (
	OffsetTimeLatest   OffsetTime = -1
	OffsetTimeEarliest OffsetTime = -2
)

var networkOrder = binary.BigEndian

// First reads the length to the size pt, then reads the string to the topic
func binread(r io.Reader, data ...interface{}) error {
	for _, d := range data {
		if err := binary.Read(r, networkOrder, d); err != nil {
			return err
		}
	}
	return nil
}

func binwrite(w io.Writer, data ...interface{}) (l int64, err error) {
	for _, d := range data {
		if err := binary.Write(w, networkOrder, d); err != nil {
			return -1, err
		}
		l += int64(binary.Size(d))
	}
	return
}

type Message []byte

func (m Message) Len() int32 {
	return int32(messageFullHeaderSize + len(m))
}

func (m Message) WriteTo(w io.Writer) (n int64, err error) {
	compression := CompressionTypeNone
	if compression != CompressionTypeNone {
		return -1, fmt.Errorf("Only support none compression for now")
	}

	totalLen := m.Len() - 4 // Subtract the size of the length
	checksum := uint32(crc32.Checksum(m, crc32.IEEETable))

	return binwrite(w, totalLen, MagicTypeWithCompression, compression, checksum, m)
}

// List of messages
type Messages []Message

func (m Messages) Len() int32 {
	l := int32(0)
	for _, m := range m {
		l += m.Len()
	}
	// Add 4 for the length overhead
	return l + 4
}

func (ms Messages) WriteTo(w io.Writer) (n int64, err error) {
	if n, err = binwrite(w, ms.Len()-4); err != nil {
		return -1, err
	}
	for i, _ := range ms {
		var nn int64
		if nn, err = ms[i].WriteTo(w); err != nil {
			return -1, err
		}
		n += nn
	}

	return
}

type OffsetsRequest struct {
	Topic     string
	Partition Partition
	Time      OffsetTime
	MaxNumber int32
}

func (req *OffsetsRequest) Len() int32 {
	return int32(2 + len([]byte(req.Topic)) + 4 + 8 + 4)
}

func (req *OffsetsRequest) WriteTo(w io.Writer) (n int64, err error) {
	if n, err = writeTopic(w, req.Topic); err != nil {
		return -1, err
	}

	var nn int64
	if nn, err = binwrite(w, req.Partition, req.Time, req.MaxNumber); err != nil {
		return -1, err
	}
	n += nn
	return
}

func (*OffsetsRequest) Type() requestType {
	return requestTypeOffsets
}

type FetchRequest struct {
	Topic     string
	Partition Partition
	Offset    Offset
	MaxSize   int32
}

// Length including size in header
func (fr *FetchRequest) Len() int32 {
	return int32(2 + len([]byte(fr.Topic)) + 4 + 8 + 4)
}

// Does not write its length or type so we can use this for both multi-and non-multi
func (req *FetchRequest) WriteTo(w io.Writer) (n int64, err error) {
	if n, err = writeTopic(w, req.Topic); err != nil {
		return -1, err
	}

	var l int64
	if l, err = binwrite(w, req.Partition, req.Offset, req.MaxSize); err != nil {
		return -1, err
	}

	n += l

	return
}

func (*FetchRequest) Type() requestType {
	return requestTypeFetch
}

type MultiFetchRequest []FetchRequest

func (m MultiFetchRequest) Len() int32 {
	l := int32(0)
	for _, m := range m {
		l += m.Len()
	}
	// Add 2 for the length overhead
	return l + 2
}

// Does not write its length or type so we can use this for both multi-and non-multi
func (reqs MultiFetchRequest) WriteTo(w io.Writer) (n int64, err error) {
	cnt := int16(len(reqs))

	// TOPICPARTITION_COUNT
	if n, err = binwrite(w, cnt); err != nil {
		return -1, err
	}

	for _, req := range reqs {
		var nn int64
		if nn, err = req.WriteTo(w); err != nil {
			return -1, err
		}
		n += nn
	}

	return
}
func (MultiFetchRequest) Type() requestType {
	return requestTypeMultiFetch
}

type ProduceRequest struct {
	Topic     string
	Partition Partition
	Messages  Messages
}

func (req *ProduceRequest) Len() int32 {
	return int32(2+len([]byte(req.Topic))+4) + req.Messages.Len() // topiclen, topic, partition, messageslen + messages
}

func (req *ProduceRequest) WriteTo(w io.Writer) (n int64, err error) {
	if n, err = writeTopic(w, req.Topic); err != nil {
		return -1, err
	}

	var nn int64
	if nn, err = binwrite(w, req.Partition); err != nil {
		return -1, err
	}
	n += nn

	if nn, err = req.Messages.WriteTo(w); err != nil {
		return -1, err
	}
	n += nn
	return
}

func (m *ProduceRequest) Type() requestType {
	return requestTypeProduce
}

type MultiProduceRequest []ProduceRequest

func (reqs MultiProduceRequest) WriteTo(w io.Writer) (n int64, err error) {
	cnt := int16(len(reqs))

	// TOPICPARTITION_COUNT
	if n, err = binwrite(w, cnt); err != nil {
		return -1, err
	}

	for _, req := range reqs {
		var nn int64
		if nn, err = req.WriteTo(w); err != nil {
			return -1, err
		}
		n += nn
	}

	return
}

func (m MultiProduceRequest) Len() int32 {
	l := int32(0)
	for _, m := range m {
		l += m.Len()
	}
	// Add 2 for the length overhead
	return l + 2
}

func (m MultiProduceRequest) Type() requestType {
	return requestTypeMultiProduce
}

const (
	// excluding length field
	messageHeaderSize = 1 + 1 + 4 // magic, compression, checksum
	// Including the length field
	messageFullHeaderSize = 4 + messageHeaderSize
)

func writeTopic(w io.Writer, topic string) (n int64, err error) {
	topicBytes := []byte(topic)
	topicLen := int16(len(topicBytes))
	return binwrite(w, topicLen, topicBytes)
}

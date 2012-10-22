package kafka

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

type TopicPartition struct {
	Topic     string
	Partition Partition
}

type TopicPartitionOffset struct {
	TopicPartition
	Offset Offset
}

type Offset int64
type Partition int32

type OffsetTime int64

const (
	OffsetTimeLatest   OffsetTime = -1
	OffsetTimeEarliest OffsetTime = -2
)

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

type Message []byte

// List of messages
type Messages []Message

const (
	// excluding length field
	messageHeaderSize = 1 + 1 + 4 // magic, compression, checksum
	// Including the length field
	messageFullHeaderSize = 4 + messageHeaderSize
)

package kafka

// make []int32 sortable so we can sort partition numbers

type int32Slice []int32

func (slice int32Slice) Len() int {
	return len(slice)
}

func (slice int32Slice) Less(i, j int) bool {
	return slice[i] < slice[j]
}

func (slice int32Slice) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// make strings encodable for convenience so they can be used as keys
// and/or values in kafka messages

// StringEncoder implements the Encoder interface for Go strings so that you can do things like
//	producer.SendMessage(nil, kafka.StringEncoder("hello world"))
type StringEncoder string

func (s StringEncoder) Encode() ([]byte, error) {
	return []byte(s), nil
}

// A simple interface for any type that can be encoded as an array of bytes
// in order to be sent as the key or value of a Kafka message.
type Encoder interface {
	Encode() ([]byte, error)
}

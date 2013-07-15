package kafka

// A simple interface for any type that can be encoded as an array of bytes
// in order to be sent as the key or value of a Kafka message.
type Encoder interface {
	Encode() ([]byte, error)
}

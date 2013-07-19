package encoding

import "fmt"

// EncodingError is returned from a failure while encoding a Kafka packet. This can happen, for example,
// if you try to encode a string over 2^15 characters in length, since Kafka's encoding rules do not permit that.
type EncodingError string

func (err EncodingError) Error() string {
	return "kafka: Could not encode packet. " + string(err)
}

// InsufficientData is returned when decoding and the packet is truncated. This can be expected
// when requesting messages, since as an optimization the server is allowed to return a partial message at the end
// of the message set.
type InsufficientData int

func (err InsufficientData) Error() string {
	return fmt.Sprintf("kafka: Insufficient data to decode packet, at least %d more bytes expected.", int(err))
}

// DecodingError is returned when there was an error (other than truncated data) decoding the Kafka broker's response.
// This can be a bad CRC or length field, or any other invalid value.
type DecodingError string

func (err DecodingError) Error() string {
	return "kafka: Could not decode packet. " + string(err)
}

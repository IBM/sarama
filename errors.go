package kafka

import "errors"

// EncodingError is returned from a failure while encoding a Kafka packet. This can happen, for example,
// if you try to encode a string over 2^15 characters in length, since Kafka's encoding rules do not permit that.
var EncodingError = errors.New("kafka: Error while encoding packet.")

// InsufficientData is returned when decoding and the packet is truncated. This can be expected
// when requesting messages, since as an optimization the server is allowed to return a partial message at the end
// of the message set.
var InsufficientData = errors.New("kafka: Insufficient data to decode packet, more bytes expected.")

// DecodingError is returned when there was an error (other than truncated data) decoding the Kafka broker's response.
// This can be a bad CRC or length field, or any other invalid value.
var DecodingError = errors.New("kafka: Error while decoding packet.")

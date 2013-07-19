/*
Package encoding provides an API for dealing with data that is encoded using Kafka's
encoding rules.

Kafka uses a custom set of encoding rules for arrays, strings, and other non-trivial data structures.
This package implements encoders and decoders for Go types in this format, as well as broader helper
functions for encoding entire structs a field at a time.
*/
package encoding

// Encoder is the interface that wraps the basic Encode method.
// Anything implementing Encoder can be turned into bytes using Kafka's encoding rules.
type Encoder interface {
	Encode(pe PacketEncoder) error
}

// Encode takes an Encoder and turns it into bytes.
func Encode(in Encoder) ([]byte, error) {
	if in == nil {
		return nil, nil
	}

	var prepEnc prepEncoder
	var realEnc realEncoder

	err := in.Encode(&prepEnc)
	if err != nil {
		return nil, err
	}

	realEnc.raw = make([]byte, prepEnc.length)
	err = in.Encode(&realEnc)
	if err != nil {
		return nil, err
	}

	return realEnc.raw, nil
}

// Decoder is the interface that wraps the basic Decode method.
// Anything implementing Decoder can be extracted from bytes using Kafka's encoding rules.
type Decoder interface {
	Decode(pd PacketDecoder) error
}

// Decode takes bytes and a Decoder and fills the fields of the decoder from the bytes,
// interpreted using Kafka's encoding rules.
func Decode(buf []byte, in Decoder) error {
	if buf == nil {
		return nil
	}

	helper := realDecoder{raw: buf}
	err := in.Decode(&helper)
	if err != nil {
		return err
	}

	if helper.off != len(buf) {
		return DecodingError
	}

	return nil
}

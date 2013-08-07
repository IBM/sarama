package kafka

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

// CompressionCodec represents the various compression codecs recognized by Kafka in messages.
type CompressionCodec int8

const (
	COMPRESSION_NONE   CompressionCodec = 0
	COMPRESSION_GZIP   CompressionCodec = 1
	COMPRESSION_SNAPPY CompressionCodec = 2
)

// The spec just says: "This is a version id used to allow backwards compatible evolution of the message
// binary format." but it doesn't say what the current value is, so presumably 0...
const message_format int8 = 0

type Message struct {
	Codec CompressionCodec // codec used to compress the message contents
	Key   []byte           // the message key, may be nil
	Value []byte           // the message contents
}

func (m *Message) encode(pe packetEncoder) error {
	pe.push(&crc32Field{})

	pe.putInt8(message_format)

	var attributes int8 = 0
	attributes |= int8(m.Codec) & 0x07
	pe.putInt8(attributes)

	err := pe.putBytes(m.Key)
	if err != nil {
		return err
	}

	var body []byte
	switch m.Codec {
	case COMPRESSION_NONE:
		body = m.Value
	case COMPRESSION_GZIP:
		if m.Value != nil {
			var buf bytes.Buffer
			writer := gzip.NewWriter(&buf)
			writer.Write(m.Value)
			writer.Close()
			body = buf.Bytes()
		}
	case COMPRESSION_SNAPPY:
		// TODO
	}
	err = pe.putBytes(body)
	if err != nil {
		return err
	}

	return pe.pop()
}

func (m *Message) decode(pd packetDecoder) (err error) {
	err = pd.push(&crc32Field{})
	if err != nil {
		return err
	}

	format, err := pd.getInt8()
	if err != nil {
		return err
	}
	if format != message_format {
		return DecodingError
	}

	attribute, err := pd.getInt8()
	if err != nil {
		return err
	}
	m.Codec = CompressionCodec(attribute & 0x07)

	m.Key, err = pd.getBytes()
	if err != nil {
		return err
	}

	m.Value, err = pd.getBytes()
	if err != nil {
		return err
	}

	switch m.Codec {
	case COMPRESSION_NONE:
		// nothing to do
	case COMPRESSION_GZIP:
		if m.Value == nil {
			return DecodingError
		}
		reader, err := gzip.NewReader(bytes.NewReader(m.Value))
		if err != nil {
			return err
		}
		m.Value, err = ioutil.ReadAll(reader)
		if err != nil {
			return err
		}
	case COMPRESSION_SNAPPY:
		// TODO
	default:
		return DecodingError
	}

	err = pd.pop()
	if err != nil {
		return err
	}

	return nil
}

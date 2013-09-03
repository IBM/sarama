package sarama

import (
	"bytes"
	"code.google.com/p/snappy-go/snappy"
	"compress/gzip"
	"io/ioutil"
)

// CompressionCodec represents the various compression codecs recognized by Kafka in messages.
type CompressionCodec uint8

const (
	CompressionNone   CompressionCodec = 0
	CompressionGZIP   CompressionCodec = 1
	CompressionSnappy CompressionCodec = 2
)

// The spec just says: "This is a version id used to allow backwards compatible evolution of the message
// binary format." but it doesn't say what the current value is, so presumably 0...
const messageFormat int8 = 0

type Message struct {
	Codec CompressionCodec // codec used to compress the message contents
	Key   []byte           // the message key, may be nil
	Value []byte           // the message contents

	compressedCache []byte
}

func (m *Message) encode(pe packetEncoder) error {
	pe.push(&crc32Field{})

	pe.putInt8(messageFormat)

	attributes := int8(m.Codec << 5)
	pe.putInt8(attributes)

	err := pe.putBytes(m.Key)
	if err != nil {
		return err
	}

	var payload []byte

	if m.compressedCache != nil {
		payload = m.compressedCache
		m.compressedCache = nil
	} else {
		switch m.Codec {
		case CompressionNone:
			payload = m.Value
		case CompressionGZIP:
			var buf bytes.Buffer
			writer := gzip.NewWriter(&buf)
			writer.Write(m.Value)
			writer.Close()
			m.compressedCache = buf.Bytes()
			payload = m.compressedCache
		case CompressionSnappy:
			tmp, err := snappy.Encode(nil, m.Value)
			if err != nil {
				return err
			}
			m.compressedCache = tmp
			payload = m.compressedCache
		default:
			return EncodingError
		}
	}

	err = pe.putBytes(payload)
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
	if format != messageFormat {
		return DecodingError
	}

	attribute, err := pd.getInt8()
	if err != nil {
		return err
	}
	m.Codec = CompressionCodec(attribute >> 5)

	m.Key, err = pd.getBytes()
	if err != nil {
		return err
	}

	m.Value, err = pd.getBytes()
	if err != nil {
		return err
	}

	switch m.Codec {
	case CompressionNone:
		// nothing to do
	case CompressionGZIP:
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
	case CompressionSnappy:
		if m.Value == nil {
			return DecodingError
		}
		m.Value, err = snappy.Decode(nil, m.Value)
		if err != nil {
			return err
		}
	default:
		return DecodingError
	}

	err = pd.pop()
	if err != nil {
		return err
	}

	return nil
}

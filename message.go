package kafka

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

type compressionCodec int

const (
	COMPRESSION_NONE   compressionCodec = 0
	COMPRESSION_GZIP   compressionCodec = 1
	COMPRESSION_SNAPPY compressionCodec = 2
)

// The spec just says: "This is a version id used to allow backwards compatible evolution of the message
// binary format." but it doesn't say what the current value is, so presumably 0...
const message_format int8 = 0

type Message struct {
	Codec compressionCodec // how  to compress the contents of the message
	Key   []byte           // the message key, may be nil
	Value []byte           // the message contents
}

func (m *Message) encode(pe packetEncoder) {
	pe.pushCRC32()

	pe.putInt8(message_format)

	var attributes int8 = 0
	attributes |= int8(m.Codec & 0x07)
	pe.putInt8(attributes)

	pe.putBytes(m.Key)

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
	pe.putBytes(body)

	pe.pop()
}

func (m *Message) decode(pd packetDecoder) (err error) {
	err = pd.pushCRC32()
	if err != nil {
		return err
	}

	format, err := pd.getInt8()
	if err != nil {
		return err
	}
	if format != message_format {
		return DecodingError("Message format mismatch.")
	}

	attribute, err := pd.getInt8()
	if err != nil {
		return err
	}
	m.Codec = compressionCodec(attribute & 0x07)

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
			return DecodingError("Nil contents cannot be compressed.")
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
		return DecodingError("Unknown compression codec.")
	}

	err = pd.pop()
	if err != nil {
		return err
	}

	return nil
}

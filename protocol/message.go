package protocol

import enc "sarama/encoding"
import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"sarama/types"
)

// The spec just says: "This is a version id used to allow backwards compatible evolution of the message
// binary format." but it doesn't say what the current value is, so presumably 0...
const message_format int8 = 0

type Message struct {
	Codec types.CompressionCodec // codec used to compress the message contents
	Key   []byte                 // the message key, may be nil
	Value []byte                 // the message contents
}

func (m *Message) Encode(pe enc.PacketEncoder) error {
	pe.Push(&enc.CRC32Field{})

	pe.PutInt8(message_format)

	var attributes int8 = 0
	attributes |= m.Codec & 0x07
	pe.PutInt8(attributes)

	err := pe.PutBytes(m.Key)
	if err != nil {
		return err
	}

	var body []byte
	switch m.Codec {
	case types.COMPRESSION_NONE:
		body = m.Value
	case types.COMPRESSION_GZIP:
		if m.Value != nil {
			var buf bytes.Buffer
			writer := gzip.NewWriter(&buf)
			writer.Write(m.Value)
			writer.Close()
			body = buf.Bytes()
		}
	case types.COMPRESSION_SNAPPY:
		// TODO
	}
	err = pe.PutBytes(body)
	if err != nil {
		return err
	}

	return pe.Pop()
}

func (m *Message) Decode(pd enc.PacketDecoder) (err error) {
	err = pd.Push(&CRC32Field{})
	if err != nil {
		return err
	}

	format, err := pd.GetInt8()
	if err != nil {
		return err
	}
	if format != message_format {
		return enc.DecodingError
	}

	attribute, err := pd.GetInt8()
	if err != nil {
		return err
	}
	m.Codec = attribute & 0x07

	m.Key, err = pd.GetBytes()
	if err != nil {
		return err
	}

	m.Value, err = pd.GetBytes()
	if err != nil {
		return err
	}

	switch m.Codec {
	case types.COMPRESSION_NONE:
		// nothing to do
	case types.COMPRESSION_GZIP:
		if m.Value == nil {
			return enc.DecodingError
		}
		reader, err := gzip.NewReader(bytes.NewReader(m.Value))
		if err != nil {
			return err
		}
		m.Value, err = ioutil.ReadAll(reader)
		if err != nil {
			return err
		}
	case types.COMPRESSION_SNAPPY:
		// TODO
	default:
		return enc.DecodingError
	}

	err = pd.Pop()
	if err != nil {
		return err
	}

	return nil
}

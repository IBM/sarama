package kafka

type compressionCodec int

const (
	COMPRESSION_NONE   compressionCodec = 0
	COMPRESSION_GZIP   compressionCodec = 1
	COMPRESSION_SNAPPY compressionCodec = 2
)

// The spec just says: "This is a version id used to allow backwards compatible evolution of the message
// binary format." but it doesn't say what the current value is, so presumably 0...
const MESSAGE_FORMAT int8 = 0

type message struct {
	codec compressionCodec
	key   *[]byte
	value *[]byte
}

func (m *message) encode(pe packetEncoder) {
	pe.pushCRC32()

	pe.putInt8(MESSAGE_FORMAT)

	var attributes int8 = 0
	attributes |= int8(m.codec & 0x07)
	pe.putInt8(attributes)

	pe.putBytes(m.key)
	pe.putBytes(m.value)

	pe.pop()
}

func (m *message) decode(pd packetDecoder) (err error) {
	err = pd.pushCRC32()
	if err != nil {
		return err
	}

	format, err := pd.getInt8()
	if err != nil {
		return err
	}
	if format != MESSAGE_FORMAT {
		return DecodingError{}
	}

	attribute, err := pd.getInt8()
	if err != nil {
		return err
	}
	m.codec = compressionCodec(attribute & 0x07)

	m.key, err = pd.getBytes()
	if err != nil {
		return err
	}

	m.value, err = pd.getBytes()
	if err != nil {
		return err
	}

	err = pd.pop()
	if err != nil {
		return err
	}

	return nil
}

func newMessageFromString(in string) *message {
	buf := make([]byte, len(in))
	return &message{value: &buf}
}

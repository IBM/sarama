package kafka

type metadata struct {
	brokers []broker
	topics  []topicMetadata
}

func (m *metadata) length() (int, error) {
	length := 4
	for i := range m.brokers {
		tmp, err := (&m.brokers[i]).length()
		if err != nil {
			return -1, err
		}
		length += tmp
	}
	length += 4
	for i := range m.topics {
		tmp, err := (&m.topics[i]).length()
		if err != nil {
			return -1, err
		}
		length += tmp
	}
	return length, nil
}

func (m *metadata) encode(buf []byte, off int) int {
	off = encodeInt32(buf, off, int32(len(m.brokers)))
	for i := range m.brokers {
		off = (&m.brokers[i]).encode(buf, off)
	}
	off = encodeInt32(buf, off, int32(len(m.topics)))
	for i := range m.topics {
		off = (&m.topics[i]).encode(buf, off)
	}
	return off
}

func (m *metadata) decode(buf []byte, off int) (int, error) {
}

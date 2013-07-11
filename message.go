package kafka

type message struct {
}

func (m *message) encode(pe packetEncoder) {
}

func (m *message) decode(pd packetDecoder) (err error) {
	return nil
}

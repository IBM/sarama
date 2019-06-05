package sarama

//MessageBlock is a message block
type MessageBlock struct {
	Offset int64
	Msg    *Message
}

// Messages convenience helper which returns either all the
// messages that are wrapped in this block
func (m *MessageBlock) Messages() []*MessageBlock {
	if m.Msg.Set != nil {
		return m.Msg.Set.Messages
	}
	return []*MessageBlock{m}
}

func (m *MessageBlock) encode(pe packetEncoder) error {
	pe.putInt64(m.Offset)
	pe.push(&lengthField{})
	err := m.Msg.encode(pe)
	if err != nil {
		return err
	}
	return pe.pop()
}

func (m *MessageBlock) decode(pd packetDecoder) (err error) {
	if m.Offset, err = pd.getInt64(); err != nil {
		return err
	}

	if err = pd.push(&lengthField{}); err != nil {
		return err
	}

	m.Msg = new(Message)
	if err = m.Msg.decode(pd); err != nil {
		return err
	}

	if err = pd.pop(); err != nil {
		return err
	}

	return nil
}

//MessageSet is a message set
type MessageSet struct {
	PartialTrailingMessage bool // whether the set on the wire contained an incomplete trailing MessageBlock
	OverflowMessage        bool // whether the set on the wire contained an overflow message
	Messages               []*MessageBlock
}

func (m *MessageSet) encode(pe packetEncoder) error {
	for i := range m.Messages {
		err := m.Messages[i].encode(pe)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MessageSet) decode(pd packetDecoder) (err error) {
	m.Messages = nil

	for pd.remaining() > 0 {
		magic, err := magicValue(pd)
		if err != nil {
			if err == ErrInsufficientData {
				m.PartialTrailingMessage = true
				return nil
			}
			return err
		}

		if magic > 1 {
			return nil
		}

		msb := new(MessageBlock)
		err = msb.decode(pd)
		switch err {
		case nil:
			m.Messages = append(m.Messages, msb)
		case ErrInsufficientData:
			// As an optimization the server is allowed to return a partial message at the
			// end of the message set. Clients should handle this case. So we just ignore such things.
			if msb.Offset == -1 {
				// This is an overflow message caused by chunked down conversion
				m.OverflowMessage = true
			} else {
				m.PartialTrailingMessage = true
			}
			return nil
		default:
			return err
		}
	}

	return nil
}

func (m *MessageSet) addMessage(msg *Message) {
	block := new(MessageBlock)
	block.Msg = msg
	m.Messages = append(m.Messages, block)
}

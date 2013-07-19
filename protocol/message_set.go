package protocol

import enc "sarama/encoding"

type MessageBlock struct {
	Offset int64
	Msg    *Message
}

func (msb *MessageBlock) Encode(pe enc.PacketEncoder) error {
	pe.PutInt64(msb.Offset)
	pe.Push(&enc.LengthField{})
	err := msb.Msg.Encode(pe)
	if err != nil {
		return err
	}
	pe.Pop()
}

func (msb *MessageBlock) Decode(pd enc.PacketDecoder) (err error) {
	msb.Offset, err = pd.GetInt64()
	if err != nil {
		return err
	}

	err = pd.Push(&enc.LengthField{})
	if err != nil {
		return err
	}

	msb.Msg = new(Message)
	err = msb.Msg.Decode(pd)
	if err != nil {
		return err
	}

	err = pd.Pop()
	if err != nil {
		return err
	}

	return nil
}

type MessageSet struct {
	PartialTrailingMessage bool // whether the set on the wire contained an incomplete trailing MessageBlock
	Messages               []*MessageBlock
}

func (ms *MessageSet) Encode(pe enc.PacketEncoder) error {
	for i := range ms.Messages {
		err := ms.Messages[i].Encode(pe)
		if err != nil {
			return err
		}
	}
}

func (ms *MessageSet) Decode(pd enc.PacketDecoder) (err error) {
	ms.Messages = nil

	for pd.Remaining() > 0 {
		msb := new(MessageBlock)
		err = msb.Decode(pd)
		switch err.(type) {
		case nil:
			ms.Messages = append(ms.Messages, msb)
		case enc.InsufficientData:
			// As an optimization the server is allowed to return a partial message at the
			// end of the message set. Clients should handle this case. So we just ignore such things.
			ms.PartialTrailingMessage = true
			return nil
		default:
			return err
		}
	}

	return nil
}

func (ms *MessageSet) addMessage(msg *Message) {
	block := new(MessageBlock)
	block.Msg = msg
	ms.Messages = append(ms.Messages, block)
}

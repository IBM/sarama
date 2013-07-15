package kafka

type messageSetBlock struct {
	offset int64
	msg    *Message
}

func (msb *messageSetBlock) encode(pe packetEncoder) {
	pe.putInt64(msb.offset)
	pe.pushLength32()
	msb.msg.encode(pe)
	pe.pop()
}

func (msb *messageSetBlock) decode(pd packetDecoder) (err error) {
	msb.offset, err = pd.getInt64()
	if err != nil {
		return err
	}

	err = pd.pushLength32()
	if err != nil {
		return err
	}

	msb.msg = new(Message)
	err = msb.msg.decode(pd)
	if err != nil {
		return err
	}

	err = pd.pop()
	if err != nil {
		return err
	}

	return nil
}

type messageSet struct {
	msgs []*messageSetBlock
}

func (ms *messageSet) encode(pe packetEncoder) {
	for i := range ms.msgs {
		ms.msgs[i].encode(pe)
	}
}

func (ms *messageSet) decode(pd packetDecoder) (err error) {
	ms.msgs = nil

	for pd.remaining() > 0 {
		msb := new(messageSetBlock)
		err = msb.decode(pd)
		if err != nil {
			return err
		}
		ms.msgs = append(ms.msgs, msb)
	}

	return nil
}

func newMessageSet() *messageSet {
	set := new(messageSet)
	set.msgs = make([]*messageSetBlock, 0)
	return set
}

func (ms *messageSet) addMessage(msg *Message) {
	block := new(messageSetBlock)
	block.msg = msg
	ms.msgs = append(ms.msgs, block)
}

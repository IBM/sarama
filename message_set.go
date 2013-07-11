package kafka

type messageSetBlock struct {
	offset int64
	msg    message
}

func (msb *messageSetBlock) encode(pe packetEncoder) {
	pe.putInt64(msb.offset)
	pe.pushLength32()
	(&msb.msg).encode(pe)
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

	err = (&msb.msg).decode(pd)
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
	ms.msgs = make([]*messageSetBlock, 0)

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

func newSingletonMessageSet(msg *message) *messageSet {
	tmp := make([]*messageSetBlock, 1)
	tmp[0] = &messageSetBlock{msg: *msg}
	return &messageSet{tmp}
}

package kafka

type messageSetBlock struct {
	offset int64
	size int32
	msg message
}

func (msb *messageSetBlock) encode(pe packetEncoder) {
	pe.putInt64(msb.offset)
	pe.putInt32(msb.size)
	(&msb.msg).encode(pe)
}

func (msb *messageSetBlock) decode(pd packetDecoder) (err error) {
	msb.offset, err = pd.getInt64()
	if err != nil {
		return err
	}

	msb.size, err = pd.getInt32()
	if err != nil {
		return err
	}

	err = (&msb.message).decode(pd)
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
	ms.msgs = make([]*messageSetBlock)

	msb = new(messageSetBlock)
	err = msb.decode(pd)
	if err != nil {
		return err
	}

	return nil
}

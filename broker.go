package kafka

type broker struct {
	nodeId int32
	host   *string
	port   int32
}

func (b *broker) encode(pe packetEncoder) {
	pe.putInt32(b.nodeId)
	pe.putString(b.host)
	pe.putInt32(b.port)
}

func (b *broker) decode(pd *packetDecoder) (err error) {
	b.nodeId, err = pd.getInt32()
	if err != nil {
		return err
	}

	b.host, err = pd.getString()
	if err != nil {
		return err
	}

	b.port, err = pd.getInt32()
	if err != nil {
		return err
	}

	return nil
}

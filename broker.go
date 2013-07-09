package kafka

type broker struct {
	nodeId int32
	host   *string
	port   int32
}

func (b *broker) build(pb packetBuilder) {
	pb.putInt32(b.nodeId)
	pb.putString(b.host)
	pb.putInt32(b.port)
}

func (b *broker) decode(pd *packetDecoder) (err error) {
	b.nodeId, err = pd.getInt32()
	if err != nil { return err }

	b.host, err = pd.getString()
	if err != nil { return err }

	b.port, err = pd.getInt32()
	if err != nil { return err }

	return nil
}

package sarama

type ConsumerMetadataRequest struct {
	ConsumerGroup string
}

func (r *ConsumerMetadataRequest) encode(pe packetEncoder) error {
	return pe.putString(r.ConsumerGroup)
}

func (r *ConsumerMetadataRequest) key() int16 {
	return 10
}

func (r *ConsumerMetadataRequest) version() int16 {
	return 0
}

package sarama

type APIVersionsRequest struct {
}

func (r *APIVersionsRequest) encode(pe packetEncoder) error {
	return nil
}

func (r *APIVersionsRequest) decode(pd packetDecoder, version int16) (err error) {
	return nil
}

func (r *APIVersionsRequest) key() int16 {
	return 18
}

func (r *APIVersionsRequest) version() int16 {
	return 0
}

func (r *APIVersionsRequest) requiredVersion() KafkaVersion {
	return V0_10_0_0
}

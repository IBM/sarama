package sarama

//APIVersionsRequest ...
type APIVersionsRequest struct {
}

func (a *APIVersionsRequest) encode(pe packetEncoder) error {
	return nil
}

func (a *APIVersionsRequest) decode(pd packetDecoder, version int16) (err error) {
	return nil
}

func (a *APIVersionsRequest) key() int16 {
	return 18
}

func (a *APIVersionsRequest) version() int16 {
	return 0
}

func (a *APIVersionsRequest) requiredVersion() KafkaVersion {
	return V0_10_0_0
}

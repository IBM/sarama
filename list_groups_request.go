package sarama

//ListGroupsRequest is a group list request
type ListGroupsRequest struct {
}

func (l *ListGroupsRequest) encode(pe packetEncoder) error {
	return nil
}

func (l *ListGroupsRequest) decode(pd packetDecoder, version int16) (err error) {
	return nil
}

func (l *ListGroupsRequest) key() int16 {
	return 16
}

func (l *ListGroupsRequest) version() int16 {
	return 0
}

func (l *ListGroupsRequest) requiredVersion() KafkaVersion {
	return V0_9_0_0
}

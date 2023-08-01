package sarama

type ListGroupsRequest struct {
	Version int16
}

func (r *ListGroupsRequest) encode(pe packetEncoder) error {
	return nil
}

func (r *ListGroupsRequest) decode(pd packetDecoder, version int16) (err error) {
	return nil
}

func (r *ListGroupsRequest) key() int16 {
	return 16
}

func (r *ListGroupsRequest) version() int16 {
	return r.Version
}

func (r *ListGroupsRequest) headerVersion() int16 {
	return 1
}

func (r *ListGroupsRequest) isValidVersion() bool {
	return r.Version >= 0 && r.Version <= 2
}

func (r *ListGroupsRequest) requiredVersion() KafkaVersion {
	switch r.Version {
	case 2:
		return V2_0_0_0
	case 1:
		return V0_11_0_0
	default:
		return V0_9_0_0
	}
}

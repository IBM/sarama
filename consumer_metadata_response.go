package sarama

type ConsumerMetadataResponse struct {
	Err             KError
	Coordinator     *Broker
	CoordinatorID   int32  // deprecated: use Coordinator.ID()
	CoordinatorHost string // deprecated: use Coordinator.Addr()
	CoordinatorPort int32  // deprecated: use Coordinator.Addr()
}

func (r *ConsumerMetadataResponse) decode(pd packetDecoder, version int16) (err error) {
	tmp := new(FindCoordinatorResponse)

	if err := tmp.decode(pd, version); err != nil {
		return err
	}

	r.Err = tmp.Err
	r.Coordinator = tmp.Coordinator
	r.CoordinatorID = tmp.CoordinatorID
	r.CoordinatorHost = tmp.CoordinatorHost
	r.CoordinatorPort = tmp.CoordinatorPort

	return nil
}

func (r *ConsumerMetadataResponse) encode(pe packetEncoder) error {
	tmp := &FindCoordinatorResponse{
		Version:         0,
		Err:             r.Err,
		Coordinator:     r.Coordinator,
		CoordinatorID:   r.CoordinatorID,
		CoordinatorHost: r.CoordinatorHost,
		CoordinatorPort: r.CoordinatorPort,
	}

	if err := tmp.encode(pe); err != nil {
		return err
	}

	return nil
}

func (r *ConsumerMetadataResponse) key() int16 {
	return 10
}

func (r *ConsumerMetadataResponse) version() int16 {
	return 0
}

func (r *ConsumerMetadataResponse) requiredVersion() KafkaVersion {
	return V0_8_2_0
}

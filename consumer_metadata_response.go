package sarama

type ConsumerMetadataResponse struct {
	Err             KError
	CoordinatorId   int32
	CoordinatorHost string
	CoordinatorPort int32
}

func (r *ConsumerMetadataResponse) decode(pd packetDecoder) (err error) {
	tmp, err := pd.getInt16()
	if err != nil {
		return err
	}
	r.Err = KError(tmp)

	r.CoordinatorId, err = pd.getInt32()
	if err != nil {
		return err
	}

	r.CoordinatorHost, err = pd.getString()
	if err != nil {
		return err
	}

	r.CoordinatorPort, err = pd.getInt32()
	if err != nil {
		return err
	}

	return nil
}

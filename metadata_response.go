package kafka

type MetadataResponse struct {
	Brokers []Broker
	Topics  []TopicMetadata
}

func (m *MetadataResponse) decode(pd packetDecoder) (err error) {
	n, err := pd.getArrayCount()
	if err != nil {
		return err
	}

	m.Brokers = make([]Broker, n)
	for i := 0; i < n; i++ {
		err = (&m.Brokers[i]).decode(pd)
		if err != nil {
			return err
		}
	}

	n, err = pd.getArrayCount()
	if err != nil {
		return err
	}

	m.Topics = make([]TopicMetadata, n)
	for i := 0; i < n; i++ {
		err = (&m.Topics[i]).decode(pd)
		if err != nil {
			return err
		}
	}

	return nil
}

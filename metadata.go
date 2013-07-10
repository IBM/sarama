package kafka

type metadata struct {
	brokers []broker
	topics  []topicMetadata

	brokerMap map[int32]*broker
}

func (m *metadata) encode(pe packetEncoder) {
	pe.putInt32(int32(len(m.brokers)))
	for i := range m.brokers {
		(&m.brokers[i]).encode(pe)
	}
	pe.putInt32(int32(len(m.topics)))
	for i := range m.topics {
		(&m.topics[i]).encode(pe)
	}
}

func (m *metadata) decode(pd packetDecoder) (err error) {
	n, err := pd.getArrayCount()
	if err != nil {
		return err
	}

	m.brokers = make([]broker, n)
	m.brokerMap = make(map[int32]*broker, n)
	for i := 0; i < n; i++ {
		err = (&m.brokers[i]).decode(pd)
		if err != nil {
			return err
		}
		m.brokerMap[m.brokers[i].nodeId] = &m.brokers[i]
	}

	n, err = pd.getArrayCount()
	if err != nil {
		return err
	}

	m.topics = make([]topicMetadata, n)
	for i := 0; i < n; i++ {
		err = (&m.topics[i]).decode(pd)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *metadata) brokerById(id int32) *broker {
	if m.brokerMap == nil {
		return nil
	}
	return m.brokerMap[id]
}

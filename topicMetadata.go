package kafka

type topicMetadata struct {
	err        kafkaError
	name       *string
	partitions []partitionMetadata
}

func (tm *topicMetadata) encode(pe packetEncoder) {
        pe.putError(tm.err)
	pe.putString(tm.name)
	pe.putInt32(int32(len(m.partitions)))
	for i := range m.partitions {
		(&m.partitions[i]).encode(pe)
	}
}

func (tm *topicMetadata) decode(pd *packetDecoder) (err error) {
	n, err := pd.getArrayCount()
	if err != nil { return err }

	m.brokers = make([]broker, n)
	for i := 0; i<n; i++ {
		err = (&m.brokers[i]).decode(pd)
		if err != nil { return err }
	}

	n, err = pd.getArrayCount()
	if err != nil { return err }

	m.topics = make([]topic, n)
	for i := 0; i<n; i++ {
		err = (&m.topics[i]).decode(pd)
		if err != nil { return err }
	}
}

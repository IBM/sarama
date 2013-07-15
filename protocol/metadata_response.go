package protocol

type PartitionMetadata struct {
	Err      KError
	Id       int32
	Leader   int32
	Replicas []int32
	Isr      []int32
}

func (pm *PartitionMetadata) decode(pd packetDecoder) (err error) {
	pm.Err, err = pd.getError()
	if err != nil {
		return err
	}

	pm.Id, err = pd.getInt32()
	if err != nil {
		return err
	}

	pm.Leader, err = pd.getInt32()
	if err != nil {
		return err
	}

	pm.Replicas, err = pd.getInt32Array()
	if err != nil {
		return err
	}

	pm.Isr, err = pd.getInt32Array()
	if err != nil {
		return err
	}

	return nil
}

type TopicMetadata struct {
	Err        KError
	Name       string
	Partitions []*PartitionMetadata
}

func (tm *TopicMetadata) decode(pd packetDecoder) (err error) {
	tm.Err, err = pd.getError()
	if err != nil {
		return err
	}

	tm.Name, err = pd.getString()
	if err != nil {
		return err
	}

	n, err := pd.getArrayCount()
	if err != nil {
		return err
	}
	tm.Partitions = make([]*PartitionMetadata, n)
	for i := 0; i < n; i++ {
		tm.Partitions[i] = new(PartitionMetadata)
		err = tm.Partitions[i].decode(pd)
		if err != nil {
			return err
		}
	}

	return nil
}

type MetadataResponse struct {
	Brokers []*Broker
	Topics  []*TopicMetadata
}

func (m *MetadataResponse) decode(pd packetDecoder) (err error) {
	n, err := pd.getArrayCount()
	if err != nil {
		return err
	}

	m.Brokers = make([]*Broker, n)
	for i := 0; i < n; i++ {
		m.Brokers[i] = new(Broker)
		err = m.Brokers[i].decode(pd)
		if err != nil {
			return err
		}
	}

	n, err = pd.getArrayCount()
	if err != nil {
		return err
	}

	m.Topics = make([]*TopicMetadata, n)
	for i := 0; i < n; i++ {
		m.Topics[i] = new(TopicMetadata)
		err = m.Topics[i].decode(pd)
		if err != nil {
			return err
		}
	}

	return nil
}

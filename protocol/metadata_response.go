package protocol

import enc "sarama/encoding"

type PartitionMetadata struct {
	Err      KError
	Id       int32
	Leader   int32
	Replicas []int32
	Isr      []int32
}

func (pm *PartitionMetadata) decode(pd enc.PacketDecoder) (err error) {
	pm.Err, err = pd.GetError()
	if err != nil {
		return err
	}

	pm.Id, err = pd.GetInt32()
	if err != nil {
		return err
	}

	pm.Leader, err = pd.GetInt32()
	if err != nil {
		return err
	}

	pm.Replicas, err = pd.GetInt32Array()
	if err != nil {
		return err
	}

	pm.Isr, err = pd.GetInt32Array()
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

func (tm *TopicMetadata) Decode(pd enc.PacketDecoder) (err error) {
	tm.Err, err = pd.GetError()
	if err != nil {
		return err
	}

	tm.Name, err = pd.GetString()
	if err != nil {
		return err
	}

	n, err := pd.GetArrayLength()
	if err != nil {
		return err
	}
	tm.Partitions = make([]*PartitionMetadata, n)
	for i := 0; i < n; i++ {
		tm.Partitions[i] = new(PartitionMetadata)
		err = tm.Partitions[i].Decode(pd)
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

func (m *MetadataResponse) Decode(pd enc.PacketDecoder) (err error) {
	n, err := pd.GetArrayLength()
	if err != nil {
		return err
	}

	m.Brokers = make([]*Broker, n)
	for i := 0; i < n; i++ {
		m.Brokers[i] = new(Broker)
		err = m.Brokers[i].Decode(pd)
		if err != nil {
			return err
		}
	}

	n, err = pd.GetArrayLength()
	if err != nil {
		return err
	}

	m.Topics = make([]*TopicMetadata, n)
	for i := 0; i < n; i++ {
		m.Topics[i] = new(TopicMetadata)
		err = m.Topics[i].Decode(pd)
		if err != nil {
			return err
		}
	}

	return nil
}

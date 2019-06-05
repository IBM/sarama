package sarama

//PartitionMetadata is for partition metadata
type PartitionMetadata struct {
	Err             KError
	ID              int32
	Leader          int32
	Replicas        []int32
	Isr             []int32
	OfflineReplicas []int32
}

func (p *PartitionMetadata) decode(pd packetDecoder, version int16) (err error) {
	tmp, err := pd.getInt16()
	if err != nil {
		return err
	}
	p.Err = KError(tmp)

	p.ID, err = pd.getInt32()
	if err != nil {
		return err
	}

	p.Leader, err = pd.getInt32()
	if err != nil {
		return err
	}

	p.Replicas, err = pd.getInt32Array()
	if err != nil {
		return err
	}

	p.Isr, err = pd.getInt32Array()
	if err != nil {
		return err
	}

	if version >= 5 {
		p.OfflineReplicas, err = pd.getInt32Array()
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *PartitionMetadata) encode(pe packetEncoder, version int16) (err error) {
	pe.putInt16(int16(p.Err))
	pe.putInt32(p.ID)
	pe.putInt32(p.Leader)

	err = pe.putInt32Array(p.Replicas)
	if err != nil {
		return err
	}

	err = pe.putInt32Array(p.Isr)
	if err != nil {
		return err
	}

	if version >= 5 {
		err = pe.putInt32Array(p.OfflineReplicas)
		if err != nil {
			return err
		}
	}

	return nil
}

//TopicMetadata is a topic metadata type
type TopicMetadata struct {
	Err        KError
	Name       string
	IsInternal bool // Only valid for Version >= 1
	Partitions []*PartitionMetadata
}

func (t *TopicMetadata) decode(pd packetDecoder, version int16) (err error) {
	tmp, err := pd.getInt16()
	if err != nil {
		return err
	}
	t.Err = KError(tmp)

	t.Name, err = pd.getString()
	if err != nil {
		return err
	}

	if version >= 1 {
		t.IsInternal, err = pd.getBool()
		if err != nil {
			return err
		}
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	t.Partitions = make([]*PartitionMetadata, n)
	for i := 0; i < n; i++ {
		t.Partitions[i] = new(PartitionMetadata)
		err = t.Partitions[i].decode(pd, version)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *TopicMetadata) encode(pe packetEncoder, version int16) (err error) {
	pe.putInt16(int16(t.Err))

	err = pe.putString(t.Name)
	if err != nil {
		return err
	}

	if version >= 1 {
		pe.putBool(t.IsInternal)
	}

	err = pe.putArrayLength(len(t.Partitions))
	if err != nil {
		return err
	}

	for _, pm := range t.Partitions {
		err = pm.encode(pe, version)
		if err != nil {
			return err
		}
	}

	return nil
}

//MetadataResponse is response for metadata request
type MetadataResponse struct {
	Version        int16
	ThrottleTimeMs int32
	Brokers        []*Broker
	ClusterID      *string
	ControllerID   int32
	Topics         []*TopicMetadata
}

func (m *MetadataResponse) decode(pd packetDecoder, version int16) (err error) {
	m.Version = version

	if version >= 3 {
		m.ThrottleTimeMs, err = pd.getInt32()
		if err != nil {
			return err
		}
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	m.Brokers = make([]*Broker, n)
	for i := 0; i < n; i++ {
		m.Brokers[i] = new(Broker)
		err = m.Brokers[i].decode(pd, version)
		if err != nil {
			return err
		}
	}

	if version >= 2 {
		m.ClusterID, err = pd.getNullableString()
		if err != nil {
			return err
		}
	}

	if version >= 1 {
		m.ControllerID, err = pd.getInt32()
		if err != nil {
			return err
		}
	} else {
		m.ControllerID = -1
	}

	n, err = pd.getArrayLength()
	if err != nil {
		return err
	}

	m.Topics = make([]*TopicMetadata, n)
	for i := 0; i < n; i++ {
		m.Topics[i] = new(TopicMetadata)
		err = m.Topics[i].decode(pd, version)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *MetadataResponse) encode(pe packetEncoder) error {
	if m.Version >= 3 {
		pe.putInt32(m.ThrottleTimeMs)
	}

	err := pe.putArrayLength(len(m.Brokers))
	if err != nil {
		return err
	}
	for _, broker := range m.Brokers {
		err = broker.encode(pe, m.Version)
		if err != nil {
			return err
		}
	}

	if m.Version >= 2 {
		err := pe.putNullableString(m.ClusterID)
		if err != nil {
			return err
		}
	}

	if m.Version >= 1 {
		pe.putInt32(m.ControllerID)
	}

	err = pe.putArrayLength(len(m.Topics))
	if err != nil {
		return err
	}
	for _, tm := range m.Topics {
		err = tm.encode(pe, m.Version)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *MetadataResponse) key() int16 {
	return 3
}

func (m *MetadataResponse) version() int16 {
	return m.Version
}

func (m *MetadataResponse) requiredVersion() KafkaVersion {
	switch m.Version {
	case 1:
		return V0_10_0_0
	case 2:
		return V0_10_1_0
	case 3, 4:
		return V0_11_0_0
	case 5:
		return V1_0_0_0
	default:
		return MinVersion
	}
}

// testing API

//AddBroker is used to add broker to metadata response
func (m *MetadataResponse) AddBroker(addr string, id int32) {
	m.Brokers = append(m.Brokers, &Broker{id: id, addr: addr})
}

//AddTopic is used to add topic to metadata response
func (m *MetadataResponse) AddTopic(topic string, err KError) *TopicMetadata {
	var tmatch *TopicMetadata

	for _, tm := range m.Topics {
		if tm.Name == topic {
			tmatch = tm
			goto foundTopic
		}
	}

	tmatch = new(TopicMetadata)
	tmatch.Name = topic
	m.Topics = append(m.Topics, tmatch)

foundTopic:

	tmatch.Err = err
	return tmatch
}

//AddTopicPartition is used to add topic partition to a metadata response
func (m *MetadataResponse) AddTopicPartition(topic string, partition, brokerID int32, replicas, isr []int32, offline []int32, err KError) {
	tmatch := m.AddTopic(topic, ErrNoError)
	var pmatch *PartitionMetadata

	for _, pm := range tmatch.Partitions {
		if pm.ID == partition {
			pmatch = pm
			goto foundPartition
		}
	}

	pmatch = new(PartitionMetadata)
	pmatch.ID = partition
	tmatch.Partitions = append(tmatch.Partitions, pmatch)

foundPartition:

	pmatch.Leader = brokerID
	pmatch.Replicas = replicas
	pmatch.Isr = isr
	pmatch.OfflineReplicas = offline
	pmatch.Err = err

}

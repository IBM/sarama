package sarama

type OffsetFetchRequest struct {
	ConsumerGroup string
	Version       int16
	partitions    map[string][]int32

	KafkaVersion *KafkaVersion
}

func (r *OffsetFetchRequest) encode(pe packetEncoder) (err error) {
	if err = pe.putString(r.ConsumerGroup); err != nil {
		return err
	}
	if err = pe.putArrayLength(len(r.partitions)); err != nil {
		return err
	}
	for topic, partitions := range r.partitions {
		if err = pe.putString(topic); err != nil {
			return err
		}
		if err = pe.putInt32Array(partitions); err != nil {
			return err
		}
	}
	return nil
}

func (r *OffsetFetchRequest) decode(pd packetDecoder) (err error) {
	if r.ConsumerGroup, err = pd.getString(); err != nil {
		return err
	}
	partitionCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if partitionCount == 0 {
		return nil
	}
	r.partitions = make(map[string][]int32)
	for i := 0; i < partitionCount; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitions, err := pd.getInt32Array()
		if err != nil {
			return err
		}
		r.partitions[topic] = partitions
	}
	return nil
}

func (r *OffsetFetchRequest) key() int16 {
	return 9
}

func (r *OffsetFetchRequest) version() int16 {
	if r.KafkaVersion.AtLeast(V0_8_2_0) {
		return 1
	} else {
		return 0
	}
}

func (r *OffsetFetchRequest) AddPartition(topic string, partitionID int32) {
	if r.partitions == nil {
		r.partitions = make(map[string][]int32)
	}

	r.partitions[topic] = append(r.partitions[topic], partitionID)
}

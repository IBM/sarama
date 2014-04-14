package sarama

type OffsetFetchRequest struct {
	ConsumerGroup string
	partitions    map[string][]int32
}

func (r *OffsetFetchRequest) encode(pe packetEncoder) error {
	err := pe.putString(r.ConsumerGroup)
	if err != nil {
		return err
	}
	err = pe.putArrayLength(len(r.partitions))
	if err != nil {
		return err
	}
	for topic, partitions := range r.partitions {
		err = pe.putString(topic)
		if err != nil {
			return err
		}
		pe.putInt32Array(partitions)
	}
	return nil
}

func (r *OffsetFetchRequest) key() int16 {
	return 9
}

func (r *OffsetFetchRequest) version() int16 {
	return 0
}

func (r *OffsetFetchRequest) AddPartition(topic string, partitionID int32) {
	if r.partitions == nil {
		r.partitions = make(map[string][]int32)
	}

	r.partitions[topic] = append(r.partitions[topic], partitionID)
}

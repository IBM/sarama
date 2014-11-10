package sarama

type OffsetFetchRequest struct {
	ConsumerGroup string
	partitions    map[string][]int32
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

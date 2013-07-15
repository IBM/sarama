package kafka

type OffsetFetchRequest struct {
	ConsumerGroup *string
	partitions    map[*string][]int32
}

func (r *OffsetFetchRequest) encode(pe packetEncoder) {
	pe.putString(r.ConsumerGroup)
	pe.putArrayCount(len(r.partitions))
	for topic, partitions := range r.partitions {
		pe.putString(topic)
		pe.putInt32Array(partitions)
	}
}

func (r *OffsetFetchRequest) key() int16 {
	return 7
}

func (r *OffsetFetchRequest) version() int16 {
	return 0
}

func (r *OffsetFetchRequest) AddPartition(topic *string, partition_id int32) {
	if r.partitions == nil {
		r.partitions = make(map[*string][]int32)
	}

	r.partitions[topic] = append(r.partitions[topic], partition_id)
}

package protocol

import enc "sarama/encoding"

type OffsetFetchRequest struct {
	ConsumerGroup string
	partitions    map[string][]int32
}

func (r *OffsetFetchRequest) Encode(pe enc.PacketEncoder) error {
	err := pe.PutString(r.ConsumerGroup)
	if err != nil {
		return err
	}
	err = pe.PutArrayLength(len(r.partitions))
	if err != nil {
		return err
	}
	for topic, partitions := range r.partitions {
		err = pe.PutString(topic)
		if err != nil {
			return err
		}
		pe.PutInt32Array(partitions)
	}
}

func (r *OffsetFetchRequest) key() int16 {
	return 7
}

func (r *OffsetFetchRequest) version() int16 {
	return 0
}

func (r *OffsetFetchRequest) AddPartition(topic string, partition_id int32) {
	if r.partitions == nil {
		r.partitions = make(map[string][]int32)
	}

	r.partitions[topic] = append(r.partitions[topic], partition_id)
}

package protocol

type offsetCommitRequestBlock struct {
	offset   int64
	metadata string
}

func (r *offsetCommitRequestBlock) encode(pe packetEncoder) {
	pe.putInt64(r.offset)
	pe.putString(r.metadata)
}

type OffsetCommitRequest struct {
	ConsumerGroup string
	blocks        map[string]map[int32]*offsetCommitRequestBlock
}

func (r *OffsetCommitRequest) encode(pe packetEncoder) {
	pe.putString(r.ConsumerGroup)
	pe.putArrayCount(len(r.blocks))
	for topic, partitions := range r.blocks {
		pe.putString(topic)
		pe.putArrayCount(len(partitions))
		for partition, block := range partitions {
			pe.putInt32(partition)
			block.encode(pe)
		}
	}
}

func (r *OffsetCommitRequest) key() int16 {
	return 6
}

func (r *OffsetCommitRequest) version() int16 {
	return 0
}

func (r *OffsetCommitRequest) AddBlock(topic string, partition_id int32, offset int64, metadata string) {
	if r.blocks == nil {
		r.blocks = make(map[string]map[int32]*offsetCommitRequestBlock)
	}

	if r.blocks[topic] == nil {
		r.blocks[topic] = make(map[int32]*offsetCommitRequestBlock)
	}

	tmp := new(offsetCommitRequestBlock)
	tmp.offset = offset
	tmp.metadata = metadata

	r.blocks[topic][partition_id] = tmp
}

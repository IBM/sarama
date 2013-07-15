package kafka

// Special values accepted by Kafka for the 'time' parameter of OffsetRequest.AddBlock().
const (
	LATEST_OFFSET    int64 = -1
	EARLIEST_OFFSETS int64 = -2
)

type offsetRequestBlock struct {
	time       int64
	maxOffsets int32
}

func (r *offsetRequestBlock) encode(pe packetEncoder) {
	pe.putInt64(r.time)
	pe.putInt32(r.maxOffsets)
}

type OffsetRequest struct {
	blocks map[*string]map[int32]*offsetRequestBlock
}

func (r *OffsetRequest) encode(pe packetEncoder) {
	pe.putInt32(-1) // replica ID is always -1 for clients
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

func (r *OffsetRequest) key() int16 {
	return 2
}

func (r *OffsetRequest) version() int16 {
	return 0
}

func (r *OffsetRequest) AddBlock(topic *string, partition_id int32, time int64, maxOffsets int32) {
	if r.blocks == nil {
		r.blocks = make(map[*string]map[int32]*offsetRequestBlock)
	}

	if r.blocks[topic] == nil {
		r.blocks[topic] = make(map[int32]*offsetRequestBlock)
	}

	tmp := new(offsetRequestBlock)
	tmp.time = time
	tmp.maxOffsets = maxOffsets

	r.blocks[topic][partition_id] = tmp
}

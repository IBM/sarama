package sarama

// OffsetTime is used in Offset Requests to ask for all messages before a certain time. Any positive int64
// value will be interpreted as milliseconds, or use the special constants defined here.
type OffsetTime int64

const (
	// LatestOffsets askes for the latest offsets.
	LatestOffsets OffsetTime = -1
	// EarliestOffset askes for the earliest available offset. Note that because offsets are pulled in descending order,
	// asking for the earliest offset will always return you a single element.
	EarliestOffset OffsetTime = -2
)

type offsetRequestBlock struct {
	time       OffsetTime
	maxOffsets int32
}

func (r *offsetRequestBlock) encode(pe packetEncoder) error {
	pe.putInt64(int64(r.time))
	pe.putInt32(r.maxOffsets)
	return nil
}

type OffsetRequest struct {
	blocks map[string]map[int32]*offsetRequestBlock
}

func (r *OffsetRequest) encode(pe packetEncoder) error {
	pe.putInt32(-1) // replica ID is always -1 for clients
	err := pe.putArrayLength(len(r.blocks))
	if err != nil {
		return err
	}
	for topic, partitions := range r.blocks {
		err = pe.putString(topic)
		if err != nil {
			return err
		}
		err = pe.putArrayLength(len(partitions))
		if err != nil {
			return err
		}
		for partition, block := range partitions {
			pe.putInt32(partition)
			err = block.encode(pe)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *OffsetRequest) key() int16 {
	return 2
}

func (r *OffsetRequest) version() int16 {
	return 0
}

func (r *OffsetRequest) AddBlock(topic string, partitionID int32, time OffsetTime, maxOffsets int32) {
	if r.blocks == nil {
		r.blocks = make(map[string]map[int32]*offsetRequestBlock)
	}

	if r.blocks[topic] == nil {
		r.blocks[topic] = make(map[int32]*offsetRequestBlock)
	}

	tmp := new(offsetRequestBlock)
	tmp.time = time
	tmp.maxOffsets = maxOffsets

	r.blocks[topic][partitionID] = tmp
}

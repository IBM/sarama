package protocol

import enc "sarama/encoding"

type offsetRequestBlock struct {
	time       int64
	maxOffsets int32
}

func (r *offsetRequestBlock) Encode(pe enc.PacketEncoder) error {
	pe.PutInt64(r.time)
	pe.PutInt32(r.maxOffsets)
	return nil
}

type OffsetRequest struct {
	blocks map[string]map[int32]*offsetRequestBlock
}

func (r *OffsetRequest) Encode(pe enc.PacketEncoder) error {
	pe.PutInt32(-1) // replica ID is always -1 for clients
	err := pe.PutArrayLength(len(r.blocks))
	if err != nil {
		return err
	}
	for topic, partitions := range r.blocks {
		err = pe.PutString(topic)
		if err != nil {
			return err
		}
		err = pe.PutArrayLength(len(partitions))
		if err != nil {
			return err
		}
		for partition, block := range partitions {
			pe.PutInt32(partition)
			err = block.Encode(pe)
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

func (r *OffsetRequest) AddBlock(topic string, partition_id int32, time int64, maxOffsets int32) {
	if r.blocks == nil {
		r.blocks = make(map[string]map[int32]*offsetRequestBlock)
	}

	if r.blocks[topic] == nil {
		r.blocks[topic] = make(map[int32]*offsetRequestBlock)
	}

	tmp := new(offsetRequestBlock)
	tmp.time = time
	tmp.maxOffsets = maxOffsets

	r.blocks[topic][partition_id] = tmp
}

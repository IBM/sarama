package protocol

import enc "sarama/encoding"

type offsetCommitRequestBlock struct {
	offset   int64
	metadata string
}

func (r *offsetCommitRequestBlock) Encode(pe enc.PacketEncoder) error {
	pe.PutInt64(r.offset)
	return pe.PutString(r.metadata)
}

type OffsetCommitRequest struct {
	ConsumerGroup string
	blocks        map[string]map[int32]*offsetCommitRequestBlock
}

func (r *OffsetCommitRequest) Encode(pe enc.PacketEncoder) error {
	err := pe.PutString(r.ConsumerGroup)
	if err != nil {
		return err
	}
	err = pe.PutArrayLength(len(r.blocks))
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

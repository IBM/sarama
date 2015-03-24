package sarama

// ReceiveTime is a special value for the timestamp field of Offset Commit Requests which
// tells the broker to set the timestamp to the time at which the request was received.
// The timestamp is only used if message version 1 is used, which requires kafka 0.8.2.
const ReceiveTime int64 = -1

type offsetCommitRequestBlock struct {
	offset    int64
	timestamp int64
	metadata  string
}

func (r *offsetCommitRequestBlock) encode(pe packetEncoder, version int16) error {
	pe.putInt64(r.offset)
	if version >= 1 {
		pe.putInt64(r.timestamp)
	}
	return pe.putString(r.metadata)
}

type OffsetCommitRequest struct {
	ConsumerGroup string
	Version       int16 // 0 (0.8.1 and later) or 1 (0.8.2 and later, includes timestamp field)
	blocks        map[string]map[int32]*offsetCommitRequestBlock
}

func (r *OffsetCommitRequest) encode(pe packetEncoder) error {
	if r.Version < 0 || r.Version > 1 {
		return PacketEncodingError{"invalid or unsupported OffsetCommitRequest version field"}
	}

	err := pe.putString(r.ConsumerGroup)
	if err != nil {
		return err
	}
	err = pe.putArrayLength(len(r.blocks))
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
			err = block.encode(pe, r.Version)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *OffsetCommitRequest) key() int16 {
	return 8
}

func (r *OffsetCommitRequest) version() int16 {
	return r.Version
}

func (r *OffsetCommitRequest) AddBlock(topic string, partitionID int32, offset int64, timestamp int64, metadata string) {
	if r.blocks == nil {
		r.blocks = make(map[string]map[int32]*offsetCommitRequestBlock)
	}

	if r.blocks[topic] == nil {
		r.blocks[topic] = make(map[int32]*offsetCommitRequestBlock)
	}

	if r.Version == 0 && timestamp != 0 {
		Logger.Println("Non-zero timestamp specified for OffsetCommitRequest v0, it will be ignored")
	}

	r.blocks[topic][partitionID] = &offsetCommitRequestBlock{offset, timestamp, metadata}
}

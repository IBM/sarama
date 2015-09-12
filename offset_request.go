package sarama

type OffsetRequestBlock struct {
	Time       int64
	MaxOffsets int32
}

func (r *OffsetRequestBlock) encode(pe packetEncoder) error {
	pe.putInt64(int64(r.Time))
	pe.putInt32(r.MaxOffsets)
	return nil
}

func (r *OffsetRequestBlock) decode(pd packetDecoder) (err error) {
	if r.Time, err = pd.getInt64(); err != nil {
		return err
	}
	if r.MaxOffsets, err = pd.getInt32(); err != nil {
		return err
	}
	return nil
}

type OffsetRequest struct {
	Blocks map[string]map[int32]*OffsetRequestBlock
}

func (r *OffsetRequest) encode(pe packetEncoder) error {
	pe.putInt32(-1) // replica ID is always -1 for clients
	err := pe.putArrayLength(len(r.Blocks))
	if err != nil {
		return err
	}
	for topic, partitions := range r.Blocks {
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
			if err = block.encode(pe); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *OffsetRequest) decode(pd packetDecoder) error {
	// Ignore replica ID
	if _, err := pd.getInt32(); err != nil {
		return err
	}
	blockCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if blockCount == 0 {
		return nil
	}
	r.Blocks = make(map[string]map[int32]*OffsetRequestBlock)
	for i := 0; i < blockCount; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitionCount, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		r.Blocks[topic] = make(map[int32]*OffsetRequestBlock)
		for j := 0; j < partitionCount; j++ {
			partition, err := pd.getInt32()
			if err != nil {
				return err
			}
			block := &OffsetRequestBlock{}
			if err := block.decode(pd); err != nil {
				return err
			}
			r.Blocks[topic][partition] = block
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

func (r *OffsetRequest) AddBlock(topic string, partitionID int32, time int64, maxOffsets int32) {
	if r.Blocks == nil {
		r.Blocks = make(map[string]map[int32]*OffsetRequestBlock)
	}

	if r.Blocks[topic] == nil {
		r.Blocks[topic] = make(map[int32]*OffsetRequestBlock)
	}

	tmp := new(OffsetRequestBlock)
	tmp.Time = time
	tmp.MaxOffsets = maxOffsets

	r.Blocks[topic][partitionID] = tmp
}

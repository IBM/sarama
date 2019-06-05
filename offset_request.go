package sarama

//offsetRequestBlock is an offset request block type
type offsetRequestBlock struct {
	time       int64
	maxOffsets int32 // Only used in version 0
}

func (o *offsetRequestBlock) encode(pe packetEncoder, version int16) error {
	pe.putInt64(int64(o.time))
	if version == 0 {
		pe.putInt32(o.maxOffsets)
	}

	return nil
}

func (o *offsetRequestBlock) decode(pd packetDecoder, version int16) (err error) {
	if o.time, err = pd.getInt64(); err != nil {
		return err
	}
	if version == 0 {
		if o.maxOffsets, err = pd.getInt32(); err != nil {
			return err
		}
	}
	return nil
}

//OffsetRequest is an offset request type
type OffsetRequest struct {
	Version        int16
	replicaID      int32
	isReplicaIDSet bool
	blocks         map[string]map[int32]*offsetRequestBlock
}

func (o *OffsetRequest) encode(pe packetEncoder) error {
	if o.isReplicaIDSet {
		pe.putInt32(o.replicaID)
	} else {
		// default replica ID is always -1 for clients
		pe.putInt32(-1)
	}

	err := pe.putArrayLength(len(o.blocks))
	if err != nil {
		return err
	}
	for topic, partitions := range o.blocks {
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
			if err = block.encode(pe, o.Version); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *OffsetRequest) decode(pd packetDecoder, version int16) error {
	o.Version = version

	replicaID, err := pd.getInt32()
	if err != nil {
		return err
	}
	if replicaID >= 0 {
		o.SetReplicaID(replicaID)
	}

	blockCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if blockCount == 0 {
		return nil
	}
	o.blocks = make(map[string]map[int32]*offsetRequestBlock)
	for i := 0; i < blockCount; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitionCount, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		o.blocks[topic] = make(map[int32]*offsetRequestBlock)
		for j := 0; j < partitionCount; j++ {
			partition, err := pd.getInt32()
			if err != nil {
				return err
			}
			block := &offsetRequestBlock{}
			if err := block.decode(pd, version); err != nil {
				return err
			}
			o.blocks[topic][partition] = block
		}
	}
	return nil
}

func (o *OffsetRequest) key() int16 {
	return 2
}

func (o *OffsetRequest) version() int16 {
	return o.Version
}

func (o *OffsetRequest) requiredVersion() KafkaVersion {
	switch o.Version {
	case 1:
		return V0_10_1_0
	default:
		return MinVersion
	}
}

//SetReplicaID sets a replica id
func (o *OffsetRequest) SetReplicaID(id int32) {
	o.replicaID = id
	o.isReplicaIDSet = true
}

//ReplicaID returns a replica id
func (o *OffsetRequest) ReplicaID() int32 {
	if o.isReplicaIDSet {
		return o.replicaID
	}
	return -1
}

//AddBlock is used to add block to an offset request
func (o *OffsetRequest) AddBlock(topic string, partitionID int32, time int64, maxOffsets int32) {
	if o.blocks == nil {
		o.blocks = make(map[string]map[int32]*offsetRequestBlock)
	}

	if o.blocks[topic] == nil {
		o.blocks[topic] = make(map[int32]*offsetRequestBlock)
	}

	tmp := new(offsetRequestBlock)
	tmp.time = time
	if o.Version == 0 {
		tmp.maxOffsets = maxOffsets
	}

	o.blocks[topic][partitionID] = tmp
}

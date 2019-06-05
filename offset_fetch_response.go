package sarama

//OffsetFetchResponseBlock is an offset fetch response block
type OffsetFetchResponseBlock struct {
	Offset      int64
	LeaderEpoch int32
	Metadata    string
	Err         KError
}

func (o *OffsetFetchResponseBlock) decode(pd packetDecoder, version int16) (err error) {
	o.Offset, err = pd.getInt64()
	if err != nil {
		return err
	}

	if version >= 5 {
		o.LeaderEpoch, err = pd.getInt32()
		if err != nil {
			return err
		}
	}

	o.Metadata, err = pd.getString()
	if err != nil {
		return err
	}

	tmp, err := pd.getInt16()
	if err != nil {
		return err
	}
	o.Err = KError(tmp)

	return nil
}

func (o *OffsetFetchResponseBlock) encode(pe packetEncoder, version int16) (err error) {
	pe.putInt64(o.Offset)

	if version >= 5 {
		pe.putInt32(o.LeaderEpoch)
	}

	err = pe.putString(o.Metadata)
	if err != nil {
		return err
	}

	pe.putInt16(int16(o.Err))

	return nil
}

//OffsetFetchResponse is an offset fetch response type
type OffsetFetchResponse struct {
	Version        int16
	ThrottleTimeMs int32
	Blocks         map[string]map[int32]*OffsetFetchResponseBlock
	Err            KError
}

func (o *OffsetFetchResponse) encode(pe packetEncoder) error {
	if o.Version >= 3 {
		pe.putInt32(o.ThrottleTimeMs)
	}

	if err := pe.putArrayLength(len(o.Blocks)); err != nil {
		return err
	}
	for topic, partitions := range o.Blocks {
		if err := pe.putString(topic); err != nil {
			return err
		}
		if err := pe.putArrayLength(len(partitions)); err != nil {
			return err
		}
		for partition, block := range partitions {
			pe.putInt32(partition)
			if err := block.encode(pe, o.Version); err != nil {
				return err
			}
		}
	}
	if o.Version >= 2 {
		pe.putInt16(int16(o.Err))
	}
	return nil
}

func (o *OffsetFetchResponse) decode(pd packetDecoder, version int16) (err error) {
	o.Version = version

	if version >= 3 {
		o.ThrottleTimeMs, err = pd.getInt32()
		if err != nil {
			return err
		}
	}

	numTopics, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	if numTopics > 0 {
		o.Blocks = make(map[string]map[int32]*OffsetFetchResponseBlock, numTopics)
		for i := 0; i < numTopics; i++ {
			name, err := pd.getString()
			if err != nil {
				return err
			}

			numBlocks, err := pd.getArrayLength()
			if err != nil {
				return err
			}

			if numBlocks == 0 {
				o.Blocks[name] = nil
				continue
			}
			o.Blocks[name] = make(map[int32]*OffsetFetchResponseBlock, numBlocks)

			for j := 0; j < numBlocks; j++ {
				id, err := pd.getInt32()
				if err != nil {
					return err
				}

				block := new(OffsetFetchResponseBlock)
				err = block.decode(pd, version)
				if err != nil {
					return err
				}
				o.Blocks[name][id] = block
			}
		}
	}

	if version >= 2 {
		kerr, err := pd.getInt16()
		if err != nil {
			return err
		}
		o.Err = KError(kerr)
	}

	return nil
}

func (o *OffsetFetchResponse) key() int16 {
	return 9
}

func (o *OffsetFetchResponse) version() int16 {
	return o.Version
}

func (o *OffsetFetchResponse) requiredVersion() KafkaVersion {
	switch o.Version {
	case 1:
		return V0_8_2_0
	case 2:
		return V0_10_2_0
	case 3:
		return V0_11_0_0
	case 4:
		return V2_0_0_0
	case 5:
		return V2_1_0_0
	default:
		return MinVersion
	}
}

//GetBlock is used to get a block from offset fetch response
func (o *OffsetFetchResponse) GetBlock(topic string, partition int32) *OffsetFetchResponseBlock {
	if o.Blocks == nil {
		return nil
	}

	if o.Blocks[topic] == nil {
		return nil
	}

	return o.Blocks[topic][partition]
}

//AddBlock is used to add a block  to offset fetch response
func (o *OffsetFetchResponse) AddBlock(topic string, partition int32, block *OffsetFetchResponseBlock) {
	if o.Blocks == nil {
		o.Blocks = make(map[string]map[int32]*OffsetFetchResponseBlock)
	}
	partitions := o.Blocks[topic]
	if partitions == nil {
		partitions = make(map[int32]*OffsetFetchResponseBlock)
		o.Blocks[topic] = partitions
	}
	partitions[partition] = block
}

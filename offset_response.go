package sarama

//OffsetResponseBlock is an offset response block
type OffsetResponseBlock struct {
	Err       KError
	Offsets   []int64 // Version 0
	Offset    int64   // Version 1
	Timestamp int64   // Version 1
}

func (o *OffsetResponseBlock) decode(pd packetDecoder, version int16) (err error) {
	tmp, err := pd.getInt16()
	if err != nil {
		return err
	}
	o.Err = KError(tmp)

	if version == 0 {
		o.Offsets, err = pd.getInt64Array()

		return err
	}

	o.Timestamp, err = pd.getInt64()
	if err != nil {
		return err
	}

	o.Offset, err = pd.getInt64()
	if err != nil {
		return err
	}

	// For backwards compatibility put the offset in the offsets array too
	o.Offsets = []int64{o.Offset}

	return nil
}

func (o *OffsetResponseBlock) encode(pe packetEncoder, version int16) (err error) {
	pe.putInt16(int16(o.Err))

	if version == 0 {
		return pe.putInt64Array(o.Offsets)
	}

	pe.putInt64(o.Timestamp)
	pe.putInt64(o.Offset)

	return nil
}

//OffsetResponse is an offset response type
type OffsetResponse struct {
	Version int16
	Blocks  map[string]map[int32]*OffsetResponseBlock
}

func (o *OffsetResponse) decode(pd packetDecoder, version int16) (err error) {
	numTopics, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	o.Blocks = make(map[string]map[int32]*OffsetResponseBlock, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.getString()
		if err != nil {
			return err
		}

		numBlocks, err := pd.getArrayLength()
		if err != nil {
			return err
		}

		o.Blocks[name] = make(map[int32]*OffsetResponseBlock, numBlocks)

		for j := 0; j < numBlocks; j++ {
			id, err := pd.getInt32()
			if err != nil {
				return err
			}

			block := new(OffsetResponseBlock)
			err = block.decode(pd, version)
			if err != nil {
				return err
			}
			o.Blocks[name][id] = block
		}
	}

	return nil
}

//GetBlock is used to get an offset response block
func (o *OffsetResponse) GetBlock(topic string, partition int32) *OffsetResponseBlock {
	if o.Blocks == nil {
		return nil
	}

	if o.Blocks[topic] == nil {
		return nil
	}

	return o.Blocks[topic][partition]
}

/*
// [0 0 0 1 ntopics
0 8 109 121 95 116 111 112 105 99 topic
0 0 0 1 npartitions
0 0 0 0 id
0 0

0 0 0 1 0 0 0 0
0 1 1 1 0 0 0 1
0 8 109 121 95 116 111 112
105 99 0 0 0 1 0 0
0 0 0 0 0 0 0 1
0 0 0 0 0 1 1 1] <nil>

*/
func (o *OffsetResponse) encode(pe packetEncoder) (err error) {
	if err = pe.putArrayLength(len(o.Blocks)); err != nil {
		return err
	}

	for topic, partitions := range o.Blocks {
		if err = pe.putString(topic); err != nil {
			return err
		}
		if err = pe.putArrayLength(len(partitions)); err != nil {
			return err
		}
		for partition, block := range partitions {
			pe.putInt32(partition)
			if err = block.encode(pe, o.version()); err != nil {
				return err
			}
		}
	}

	return nil
}

func (o *OffsetResponse) key() int16 {
	return 2
}

func (o *OffsetResponse) version() int16 {
	return o.Version
}

func (o *OffsetResponse) requiredVersion() KafkaVersion {
	switch o.Version {
	case 1:
		return V0_10_1_0
	default:
		return MinVersion
	}
}

// testing API

//AddTopicPartition is used to add a topic partition to offset response
func (o *OffsetResponse) AddTopicPartition(topic string, partition int32, offset int64) {
	if o.Blocks == nil {
		o.Blocks = make(map[string]map[int32]*OffsetResponseBlock)
	}
	byTopic, ok := o.Blocks[topic]
	if !ok {
		byTopic = make(map[int32]*OffsetResponseBlock)
		o.Blocks[topic] = byTopic
	}
	byTopic[partition] = &OffsetResponseBlock{Offsets: []int64{offset}, Offset: offset}
}

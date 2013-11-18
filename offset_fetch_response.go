package sarama

type OffsetFetchResponseBlock struct {
	Offset   int64
	Metadata string
	Err      KError
}

func (r *OffsetFetchResponseBlock) decode(pd packetDecoder) (err error) {
	r.Offset, err = pd.getInt64()
	if err != nil {
		return err
	}

	r.Metadata, err = pd.getString()
	if err != nil {
		return err
	}

	tmp, err := pd.getInt16()
	if err != nil {
		return err
	}
	r.Err = KError(tmp)

	return nil
}

func (r *OffsetFetchResponseBlock) encode(pe packetEncoder) (err error) {
	pe.putInt64(r.Offset)

	err = pe.putString(r.Metadata)
	if err != nil {
		return err
	}

	pe.putInt16(int16(r.Err))

	return nil
}

type OffsetFetchResponse struct {
	ClientID string
	Blocks   map[string]map[int32]*OffsetFetchResponseBlock
}

func (r *OffsetFetchResponse) decode(pd packetDecoder) (err error) {
	r.ClientID, err = pd.getString()
	if err != nil {
		return err
	}

	numTopics, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	r.Blocks = make(map[string]map[int32]*OffsetFetchResponseBlock, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.getString()
		if err != nil {
			return err
		}

		numBlocks, err := pd.getArrayLength()
		if err != nil {
			return err
		}

		r.Blocks[name] = make(map[int32]*OffsetFetchResponseBlock, numBlocks)

		for j := 0; j < numBlocks; j++ {
			id, err := pd.getInt32()
			if err != nil {
				return err
			}

			block := new(OffsetFetchResponseBlock)
			err = block.decode(pd)
			if err != nil {
				return err
			}
			r.Blocks[name][id] = block
		}
	}

	return nil
}

func (r *OffsetFetchResponse) encode(pe packetEncoder) (err error) {
	err = pe.putString(r.ClientID)
	if err != nil {
		return err
	}

	err = pe.putArrayLength(len(r.Blocks))
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

		for id, block := range partitions {
			pe.putInt32(id)
			err = block.encode(pe)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// testing API

func (r *OffsetFetchResponse) AddTopicPartition(topic string, partition int32, offset int64) {
	byTopic, ok := r.Blocks[topic]
	if !ok {
		byTopic = make(map[int32]*OffsetFetchResponseBlock)
		r.Blocks[topic] = byTopic
	}
	byTopic[partition] = &OffsetFetchResponseBlock{Offset: offset}
}

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
	Blocks map[string]map[int32]*OffsetFetchResponseBlock
}

func (r *OffsetFetchResponse) decode(pd packetDecoder) (err error) {
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

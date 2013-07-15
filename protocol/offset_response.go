package protocol

type OffsetResponseBlock struct {
	err     KError
	offsets []int64
}

func (r *OffsetResponseBlock) decode(pd packetDecoder) (err error) {
	r.err, err = pd.getError()
	if err != nil {
		return err
	}

	r.offsets, err = pd.getInt64Array()

	return err
}

type OffsetResponse struct {
	Blocks map[string]map[int32]*OffsetResponseBlock
}

func (r *OffsetResponse) decode(pd packetDecoder) (err error) {
	numTopics, err := pd.getArrayCount()
	if err != nil {
		return err
	}

	r.Blocks = make(map[string]map[int32]*OffsetResponseBlock, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.getString()
		if err != nil {
			return err
		}

		numBlocks, err := pd.getArrayCount()
		if err != nil {
			return err
		}

		r.Blocks[name] = make(map[int32]*OffsetResponseBlock, numBlocks)

		for j := 0; j < numBlocks; j++ {
			id, err := pd.getInt32()
			if err != nil {
				return err
			}

			block := new(OffsetResponseBlock)
			err = block.decode(pd)
			if err != nil {
				return err
			}
			r.Blocks[name][id] = block
		}
	}

	return nil
}

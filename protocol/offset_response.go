package protocol

import enc "sarama/encoding"

type OffsetResponseBlock struct {
	Err     KError
	Offsets []int64
}

func (r *OffsetResponseBlock) Decode(pd enc.PacketDecoder) (err error) {
	r.Err, err = pd.GetError()
	if err != nil {
		return err
	}

	r.Offsets, err = pd.GetInt64Array()

	return err
}

type OffsetResponse struct {
	Blocks map[string]map[int32]*OffsetResponseBlock
}

func (r *OffsetResponse) Decode(pd enc.PacketDecoder) (err error) {
	numTopics, err := pd.GetArrayLength()
	if err != nil {
		return err
	}

	r.Blocks = make(map[string]map[int32]*OffsetResponseBlock, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.GetString()
		if err != nil {
			return err
		}

		numBlocks, err := pd.GetArrayLength()
		if err != nil {
			return err
		}

		r.Blocks[name] = make(map[int32]*OffsetResponseBlock, numBlocks)

		for j := 0; j < numBlocks; j++ {
			id, err := pd.GetInt32()
			if err != nil {
				return err
			}

			block := new(OffsetResponseBlock)
			err = block.Decode(pd)
			if err != nil {
				return err
			}
			r.Blocks[name][id] = block
		}
	}

	return nil
}

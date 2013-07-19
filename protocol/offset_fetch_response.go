package protocol

import enc "sarama/encoding"

type OffsetFetchResponseBlock struct {
	Offset   int64
	Metadata string
	Err      KError
}

func (r *OffsetFetchResponseBlock) Decode(pd enc.PacketDecoder) (err error) {
	r.Offset, err = pd.GetInt64()
	if err != nil {
		return err
	}

	r.Metadata, err = pd.GetString()
	if err != nil {
		return err
	}

	r.Err, err = pd.GetError()

	return err
}

type OffsetFetchResponse struct {
	ClientID string
	Blocks   map[string]map[int32]*OffsetFetchResponseBlock
}

func (r *OffsetFetchResponse) Decode(pd enc.PacketDecoder) (err error) {
	r.ClientID, err = pd.GetString()
	if err != nil {
		return err
	}

	numTopics, err := pd.GetArrayLength()
	if err != nil {
		return err
	}

	r.Blocks = make(map[string]map[int32]*OffsetFetchResponseBlock, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.GetString()
		if err != nil {
			return err
		}

		numBlocks, err := pd.GetArrayLength()
		if err != nil {
			return err
		}

		r.Blocks[name] = make(map[int32]*OffsetFetchResponseBlock, numBlocks)

		for j := 0; j < numBlocks; j++ {
			id, err := pd.GetInt32()
			if err != nil {
				return err
			}

			block := new(OffsetFetchResponseBlock)
			err = block.Decode(pd)
			if err != nil {
				return err
			}
			r.Blocks[name][id] = block
		}
	}

	return nil
}

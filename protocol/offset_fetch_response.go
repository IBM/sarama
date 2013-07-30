package protocol

import enc "sarama/encoding"
import "sarama/types"

type OffsetFetchResponseBlock struct {
	Offset   int64
	Metadata string
	Err      types.KError
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

	tmp, err := pd.GetInt16()
	if err != nil {
		return err
	}
	r.Err = types.KError(tmp)

	return nil
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

func (r *OffsetFetchResponse) GetBlock(topic string, partition int32) *OffsetFetchResponseBlock {
	if r.Blocks == nil {
		return nil
	}

	if r.Blocks[topic] == nil {
		return nil
	}

	return r.Blocks[topic][partition]
}

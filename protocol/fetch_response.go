package protocol

import enc "sarama/encoding"
import "sarama/types"

type FetchResponseBlock struct {
	Err                 types.KError
	HighWaterMarkOffset int64
	MsgSet              MessageSet
}

func (pr *FetchResponseBlock) Decode(pd enc.PacketDecoder) (err error) {
	tmp, err := pd.GetInt16()
	if err != nil {
		return err
	}
	pr.Err = types.KError(tmp)

	pr.HighWaterMarkOffset, err = pd.GetInt64()
	if err != nil {
		return err
	}

	msgSetSize, err := pd.GetInt32()
	if err != nil {
		return err
	}

	msgSetDecoder, err := pd.GetSubset(int(msgSetSize))
	if err != nil {
		return err
	}
	err = (&pr.MsgSet).Decode(msgSetDecoder)

	return err
}

type FetchResponse struct {
	Blocks map[string]map[int32]*FetchResponseBlock
}

func (fr *FetchResponse) Decode(pd enc.PacketDecoder) (err error) {
	numTopics, err := pd.GetArrayLength()
	if err != nil {
		return err
	}

	fr.Blocks = make(map[string]map[int32]*FetchResponseBlock, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.GetString()
		if err != nil {
			return err
		}

		numBlocks, err := pd.GetArrayLength()
		if err != nil {
			return err
		}

		fr.Blocks[name] = make(map[int32]*FetchResponseBlock, numBlocks)

		for j := 0; j < numBlocks; j++ {
			id, err := pd.GetInt32()
			if err != nil {
				return err
			}

			block := new(FetchResponseBlock)
			err = block.Decode(pd)
			if err != nil {
				return err
			}
			fr.Blocks[name][id] = block
		}
	}

	return nil
}

func (fr *FetchResponse) GetBlock(topic string, partition int32) *FetchResponseBlock {
	if fr.Blocks == nil {
		return nil
	}

	if fr.Blocks[topic] == nil {
		return nil
	}

	return fr.Blocks[topic][partition]
}

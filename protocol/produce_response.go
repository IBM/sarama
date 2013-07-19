package protocol

import enc "sarama/encoding"

type ProduceResponseBlock struct {
	Err    KError
	Offset int64
}

func (pr *ProduceResponseBlock) Decode(pd enc.PacketDecoder) (err error) {
	pr.Err, err = pd.GetError()
	if err != nil {
		return err
	}

	pr.Offset, err = pd.GetInt64()
	if err != nil {
		return err
	}

	return nil
}

type ProduceResponse struct {
	Blocks map[string]map[int32]*ProduceResponseBlock
}

func (pr *ProduceResponse) Decode(pd enc.PacketDecoder) (err error) {
	numTopics, err := pd.GetArrayLength()
	if err != nil {
		return err
	}

	pr.Blocks = make(map[string]map[int32]*ProduceResponseBlock, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.GetString()
		if err != nil {
			return err
		}

		numBlocks, err := pd.GetArrayLength()
		if err != nil {
			return err
		}

		pr.Blocks[name] = make(map[int32]*ProduceResponseBlock, numBlocks)

		for j := 0; j < numBlocks; j++ {
			id, err := pd.GetInt32()
			if err != nil {
				return err
			}

			block := new(ProduceResponseBlock)
			err = block.Decode(pd)
			if err != nil {
				return err
			}
			pr.Blocks[name][id] = block
		}
	}

	return nil
}

func (pr *ProduceResponse) GetBlock(topic string, partition int32) *ProduceResponseBlock {
	if pr.Blocks == nil {
		return nil
	}

	if pr.Blocks[topic] == nil {
		return nil
	}

	return pr.Blocks[topic][partition]
}

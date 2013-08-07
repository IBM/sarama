package kafka

type ProduceResponseBlock struct {
	Err    KError
	Offset int64
}

func (pr *ProduceResponseBlock) decode(pd packetDecoder) (err error) {
	tmp, err := pd.getInt16()
	if err != nil {
		return err
	}
	pr.Err = KError(tmp)

	pr.Offset, err = pd.getInt64()
	if err != nil {
		return err
	}

	return nil
}

type ProduceResponse struct {
	Blocks map[string]map[int32]*ProduceResponseBlock
}

func (pr *ProduceResponse) decode(pd packetDecoder) (err error) {
	numTopics, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	pr.Blocks = make(map[string]map[int32]*ProduceResponseBlock, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.getString()
		if err != nil {
			return err
		}

		numBlocks, err := pd.getArrayLength()
		if err != nil {
			return err
		}

		pr.Blocks[name] = make(map[int32]*ProduceResponseBlock, numBlocks)

		for j := 0; j < numBlocks; j++ {
			id, err := pd.getInt32()
			if err != nil {
				return err
			}

			block := new(ProduceResponseBlock)
			err = block.decode(pd)
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

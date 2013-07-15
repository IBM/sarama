package protocol

type ProduceResponseBlock struct {
	Err    KError
	Offset int64
}

func (pr *ProduceResponseBlock) decode(pd packetDecoder) (err error) {
	pr.Err, err = pd.getError()
	if err != nil {
		return err
	}

	pr.Offset, err = pd.getInt64()
	if err != nil {
		return err
	}

	return nil
}

type ProduceResponse struct {
	Blocks map[*string]map[int32]*ProduceResponseBlock
}

func (pr *ProduceResponse) decode(pd packetDecoder) (err error) {
	numTopics, err := pd.getArrayCount()
	if err != nil {
		return err
	}

	pr.Blocks = make(map[*string]map[int32]*ProduceResponseBlock, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.getString()
		if err != nil {
			return err
		}

		numBlocks, err := pd.getArrayCount()
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

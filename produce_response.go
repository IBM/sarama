package sarama

type ProduceResponseBlock struct {
	Err       KError
	Offset    int64
	Timestamp int64 // only provided if Version >= 2
}

func (pr *ProduceResponseBlock) decode(pd packetDecoder, version int16) (err error) {
	tmp, err := pd.getInt16()
	if err != nil {
		return err
	}
	pr.Err = KError(tmp)

	pr.Offset, err = pd.getInt64()
	if err != nil {
		return err
	}

	if version >= 2 {
		if pr.Timestamp, err = pd.getInt64(); err != nil {
			return err
		}
	}

	return nil
}

type ProduceResponse struct {
	Blocks       map[string]map[int32]*ProduceResponseBlock
	Version      int16
	ThrottleTime int32 // only provided if Version >= 1
}

func (pr *ProduceResponse) decode(pd packetDecoder, version int16) (err error) {
	pr.Version = version

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
			err = block.decode(pd, version)
			if err != nil {
				return err
			}
			pr.Blocks[name][id] = block
		}
	}

	if pr.Version >= 1 {
		if pr.ThrottleTime, err = pd.getInt32(); err != nil {
			return err
		}
	}

	return nil
}

func (pr *ProduceResponse) encode(pe packetEncoder) error {
	err := pe.putArrayLength(len(pr.Blocks))
	if err != nil {
		return err
	}
	for topic, partitions := range pr.Blocks {
		err = pe.putString(topic)
		if err != nil {
			return err
		}
		err = pe.putArrayLength(len(partitions))
		if err != nil {
			return err
		}
		for id, prb := range partitions {
			pe.putInt32(id)
			pe.putInt16(int16(prb.Err))
			pe.putInt64(prb.Offset)
		}
	}
	return nil
}

func (r *ProduceResponse) key() int16 {
	return 0
}

func (r *ProduceResponse) version() int16 {
	return r.Version
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

// Testing API

func (pr *ProduceResponse) AddTopicPartition(topic string, partition int32, err KError) {
	if pr.Blocks == nil {
		pr.Blocks = make(map[string]map[int32]*ProduceResponseBlock)
	}
	byTopic, ok := pr.Blocks[topic]
	if !ok {
		byTopic = make(map[int32]*ProduceResponseBlock)
		pr.Blocks[topic] = byTopic
	}
	byTopic[partition] = &ProduceResponseBlock{Err: err}
}

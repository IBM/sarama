package sarama

type FetchRequestBlock struct {
	FetchOffset int64
	MaxBytes    int32
}

func (f *FetchRequestBlock) encode(pe packetEncoder) error {
	pe.putInt64(f.FetchOffset)
	pe.putInt32(f.MaxBytes)
	return nil
}

func (f *FetchRequestBlock) decode(pd packetDecoder) (err error) {
	if f.FetchOffset, err = pd.getInt64(); err != nil {
		return err
	}
	if f.MaxBytes, err = pd.getInt32(); err != nil {
		return err
	}
	return nil
}

type FetchRequest struct {
	MaxWaitTime int32
	MinBytes    int32
	Blocks      map[string]map[int32]*FetchRequestBlock
}

func (f *FetchRequest) encode(pe packetEncoder) (err error) {
	pe.putInt32(-1) // replica ID is always -1 for clients
	pe.putInt32(f.MaxWaitTime)
	pe.putInt32(f.MinBytes)
	err = pe.putArrayLength(len(f.Blocks))
	if err != nil {
		return err
	}
	for topic, Blocks := range f.Blocks {
		err = pe.putString(topic)
		if err != nil {
			return err
		}
		err = pe.putArrayLength(len(Blocks))
		if err != nil {
			return err
		}
		for partition, block := range Blocks {
			pe.putInt32(partition)
			err = block.encode(pe)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *FetchRequest) decode(pd packetDecoder) (err error) {
	if _, err = pd.getInt32(); err != nil {
		return err
	}
	if f.MaxWaitTime, err = pd.getInt32(); err != nil {
		return err
	}
	if f.MinBytes, err = pd.getInt32(); err != nil {
		return err
	}
	topicCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if topicCount == 0 {
		return nil
	}
	f.Blocks = make(map[string]map[int32]*FetchRequestBlock)
	for i := 0; i < topicCount; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitionCount, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		f.Blocks[topic] = make(map[int32]*FetchRequestBlock)
		for j := 0; j < partitionCount; j++ {
			partition, err := pd.getInt32()
			if err != nil {
				return err
			}
			fetchBlock := &FetchRequestBlock{}
			if err = fetchBlock.decode(pd); err != nil {
				return nil
			}
			f.Blocks[topic][partition] = fetchBlock
		}
	}
	return nil
}

func (f *FetchRequest) key() int16 {
	return 1
}

func (f *FetchRequest) version() int16 {
	return 0
}

func (f *FetchRequest) AddBlock(topic string, partitionID int32, fetchOffset int64, maxBytes int32) {
	if f.Blocks == nil {
		f.Blocks = make(map[string]map[int32]*FetchRequestBlock)
	}

	if f.Blocks[topic] == nil {
		f.Blocks[topic] = make(map[int32]*FetchRequestBlock)
	}

	tmp := new(FetchRequestBlock)
	tmp.MaxBytes = maxBytes
	tmp.FetchOffset = fetchOffset

	f.Blocks[topic][partitionID] = tmp
}

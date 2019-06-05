package sarama

type fetchRequestBlock struct {
	fetchOffset int64
	maxBytes    int32
}

func (b *fetchRequestBlock) encode(pe packetEncoder) error {
	pe.putInt64(b.fetchOffset)
	pe.putInt32(b.maxBytes)
	return nil
}

func (b *fetchRequestBlock) decode(pd packetDecoder) (err error) {
	if b.fetchOffset, err = pd.getInt64(); err != nil {
		return err
	}
	if b.maxBytes, err = pd.getInt32(); err != nil {
		return err
	}
	return nil
}

// FetchRequest (API key 1) will fetch Kafka messages. Version 3 introduced the MaxBytes field. See
// https://issues.apache.org/jira/browse/KAFKA-2063 for a discussion of the issues leading up to that.  The KIP is at
// https://cwiki.apache.org/confluence/display/KAFKA/KIP-74%3A+Add+Fetch+Response+Size+Limit+in+Bytes
type FetchRequest struct {
	MaxWaitTime int32
	MinBytes    int32
	MaxBytes    int32
	Version     int16
	Isolation   IsolationLevel
	blocks      map[string]map[int32]*fetchRequestBlock
}

//IsolationLevel is used to define isolation level
type IsolationLevel int8

const (
	//ReadUncommitted is an isolation level
	ReadUncommitted IsolationLevel = iota
	//ReadCommitted is an isolation level
	ReadCommitted
)

func (f *FetchRequest) encode(pe packetEncoder) (err error) {
	pe.putInt32(-1) // replica ID is always -1 for clients
	pe.putInt32(f.MaxWaitTime)
	pe.putInt32(f.MinBytes)
	if f.Version >= 3 {
		pe.putInt32(f.MaxBytes)
	}
	if f.Version >= 4 {
		pe.putInt8(int8(f.Isolation))
	}
	err = pe.putArrayLength(len(f.blocks))
	if err != nil {
		return err
	}
	for topic, blocks := range f.blocks {
		err = pe.putString(topic)
		if err != nil {
			return err
		}
		err = pe.putArrayLength(len(blocks))
		if err != nil {
			return err
		}
		for partition, block := range blocks {
			pe.putInt32(partition)
			err = block.encode(pe)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *FetchRequest) decode(pd packetDecoder, version int16) (err error) {
	f.Version = version
	if _, err = pd.getInt32(); err != nil {
		return err
	}
	if f.MaxWaitTime, err = pd.getInt32(); err != nil {
		return err
	}
	if f.MinBytes, err = pd.getInt32(); err != nil {
		return err
	}
	if f.Version >= 3 {
		if f.MaxBytes, err = pd.getInt32(); err != nil {
			return err
		}
	}
	if f.Version >= 4 {
		isolation, err := pd.getInt8()
		if err != nil {
			return err
		}
		f.Isolation = IsolationLevel(isolation)
	}
	topicCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if topicCount == 0 {
		return nil
	}
	f.blocks = make(map[string]map[int32]*fetchRequestBlock)
	for i := 0; i < topicCount; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitionCount, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		f.blocks[topic] = make(map[int32]*fetchRequestBlock)
		for j := 0; j < partitionCount; j++ {
			partition, err := pd.getInt32()
			if err != nil {
				return err
			}
			fetchBlock := &fetchRequestBlock{}
			if err = fetchBlock.decode(pd); err != nil {
				return err
			}
			f.blocks[topic][partition] = fetchBlock
		}
	}
	return nil
}

func (f *FetchRequest) key() int16 {
	return 1
}

func (f *FetchRequest) version() int16 {
	return f.Version
}

func (f *FetchRequest) requiredVersion() KafkaVersion {
	switch f.Version {
	case 1:
		return V0_9_0_0
	case 2:
		return V0_10_0_0
	case 3:
		return V0_10_1_0
	case 4:
		return V0_11_0_0
	default:
		return MinVersion
	}
}

//AddBlock is used to add a block to fetch request
func (f *FetchRequest) AddBlock(topic string, partitionID int32, fetchOffset int64, maxBytes int32) {
	if f.blocks == nil {
		f.blocks = make(map[string]map[int32]*fetchRequestBlock)
	}

	if f.blocks[topic] == nil {
		f.blocks[topic] = make(map[int32]*fetchRequestBlock)
	}

	tmp := new(fetchRequestBlock)
	tmp.maxBytes = maxBytes
	tmp.fetchOffset = fetchOffset

	f.blocks[topic][partitionID] = tmp
}

package protocol

type fetchRequestBlock struct {
	fetchOffset int64
	maxBytes    int32
}

func (f *fetchRequestBlock) encode(pe packetEncoder) {
	pe.putInt64(f.fetchOffset)
	pe.putInt32(f.maxBytes)
}

type FetchRequest struct {
	MaxWaitTime int32
	MinBytes    int32
	blocks      map[string]map[int32]*fetchRequestBlock
}

func (f *FetchRequest) encode(pe packetEncoder) {
	pe.putInt32(-1) // replica ID is always -1 for clients
	pe.putInt32(f.MaxWaitTime)
	pe.putInt32(f.MinBytes)
	pe.putArrayCount(len(f.blocks))
	for topic, blocks := range f.blocks {
		pe.putString(topic)
		pe.putArrayCount(len(blocks))
		for partition, block := range blocks {
			pe.putInt32(partition)
			block.encode(pe)
		}
	}
}

func (f *FetchRequest) key() int16 {
	return 1
}

func (f *FetchRequest) version() int16 {
	return 0
}

func (f *FetchRequest) AddBlock(topic string, partition_id int32, fetchOffset int64, maxBytes int32) {
	if f.blocks == nil {
		f.blocks = make(map[string]map[int32]*fetchRequestBlock)
	}

	if f.blocks[topic] == nil {
		f.blocks[topic] = make(map[int32]*fetchRequestBlock)
	}

	tmp := new(fetchRequestBlock)
	tmp.maxBytes = maxBytes
	tmp.fetchOffset = fetchOffset

	f.blocks[topic][partition_id] = tmp
}

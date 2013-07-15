package kafka

type fetchRequestPartition struct {
	fetchOffset int64
	maxBytes    int32
}

func (f *fetchRequestPartition) encode(pe packetEncoder) {
	pe.putInt64(f.fetchOffset)
	pe.putInt32(f.maxBytes)
}

type FetchRequest struct {
	MaxWaitTime int32
	MinBytes    int32
	partitions  map[*string]map[int32]*fetchRequestPartition
}

func (f *FetchRequest) encode(pe packetEncoder) {
	pe.putInt32(-1) // replica ID is always -1 for clients
	pe.putInt32(f.MaxWaitTime)
	pe.putInt32(f.MinBytes)
	pe.putArrayCount(len(f.partitions))
	for topic, partitions := range f.partitions {
		pe.putString(topic)
		pe.putArrayCount(len(partitions))
		for partition, block := range partitions {
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

func (f *FetchRequest) AddPartition(topic *string, partition_id int32, fetchOffset int64, maxBytes int32) {
	if f.partitions == nil {
		f.partitions = make(map[*string]map[int32]*fetchRequestPartition)
	}

	if f.partitions[topic] == nil {
		f.partitions[topic] = make(map[int32]*fetchRequestPartition)
	}

	tmp := new(fetchRequestPartition)
	tmp.maxBytes = maxBytes
	tmp.fetchOffset = fetchOffset

	f.partitions[topic][partition_id] = tmp
}

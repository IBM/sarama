package kafka

type fetchRequestPartitionBlock struct {
	partition   int32
	fetchOffset int64
	maxBytes    int32
}

func (p *fetchRequestPartitionBlock) encode(pe packetEncoder) {
	pe.putInt32(p.partition)
	pe.putInt64(p.fetchOffset)
	pe.putInt32(p.maxBytes)
}

type fetchRequestTopicBlock struct {
	topic      *string
	partitions []fetchRequestPartitionBlock
}

func (p *fetchRequestTopicBlock) encode(pe packetEncoder) {
	pe.putString(p.topic)
	pe.putArrayCount(len(p.partitions))
	for i := range p.partitions {
		(&p.partitions[i]).encode(pe)
	}
}

type fetchRequest struct {
	maxWaitTime int32
	minBytes    int32
	topics      []fetchRequestTopicBlock
}

func (p *fetchRequest) encode(pe packetEncoder) {
	pe.putInt32(-1) // replica ID is always -1 for clients
	pe.putInt32(p.maxWaitTime)
	pe.putInt32(p.minBytes)
	pe.putArrayCount(len(p.topics))
	for i := range p.topics {
		(&p.topics[i]).encode(pe)
	}
}

func (p *fetchRequest) key() int16 {
	return 1
}

func (p *fetchRequest) version() int16 {
	return 0
}

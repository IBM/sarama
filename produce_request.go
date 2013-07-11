package kafka

type produceRequestPartitionBlock struct {
	partition int32
	msgSet    *messageSet
}

func (p *produceRequestPartitionBlock) encode(pe packetEncoder) {
	pe.putInt32(p.partition)
	pe.pushLength32()
	p.msgSet.encode(pe)
	pe.pop()
}

type produceRequestTopicBlock struct {
	topic      *string
	partitions []produceRequestPartitionBlock
}

func (p *produceRequestTopicBlock) encode(pe packetEncoder) {
	pe.putString(p.topic)
	pe.putArrayCount(len(p.partitions))
	for i := range p.partitions {
		(&p.partitions[i]).encode(pe)
	}
}

type produceRequest struct {
	requiredAcks int16
	timeout      int32
	topics       []produceRequestTopicBlock
}

func (p *produceRequest) encode(pe packetEncoder) {
	pe.putInt16(p.requiredAcks)
	pe.putInt32(p.timeout)
	pe.putArrayCount(len(p.topics))
	for i := range p.topics {
		(&p.topics[i]).encode(pe)
	}
}

func newSingletonProduceRequest(topic string, partition int32, set *messageSet) *produceRequest {
	req := &produceRequest{topics: make([]produceRequestTopicBlock, 1)}
	req.topics[0].topic = &topic
	req.topics[0].partitions = make([]produceRequestPartitionBlock, 1)
	req.topics[0].partitions[0].partition = partition
	req.topics[0].partitions[0].msgSet = set
	return req
}

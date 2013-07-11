package kafka

type produceRequestPartitionBlock struct {
	partition int32
	msgSet    *messageSet
}

type produceRequestTopicBlock struct {
	topic      *string
	partitions []produceRequestPartitionBlock
}

type produceRequest struct {
	requiredAcks int16
	timeout      int32
	topics       []produceRequestTopicBlock
}

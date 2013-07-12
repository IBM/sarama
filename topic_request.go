package kafka

type topicRequest interface {
	name() *string
	partitions() []partitionRequest
}

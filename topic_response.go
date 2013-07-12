package kafka

type topicResponse interface {
	decoder
	name() *string
	partitions() []partitionResponse
}

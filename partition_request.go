package kafka

type partitionRequest interface {
	encoder
	id() int32
}

package kafka

type partitionResponse interface {
	decoder
	id() int32
	err() KError
}

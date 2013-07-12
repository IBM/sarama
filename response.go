package kafka

type response interface {
	decoder
	topics() []topicResponse
}

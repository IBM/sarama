package kafka

type responseAPI interface {
	staleTopics() []*string
}

type responseDecoder interface {
	decoder
	responseAPI
}

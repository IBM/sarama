package kafka

type Producer struct {
	client *Client
	topic  string
}

func NewProducer(client *Client, topic string) *Producer {
	return &Producer{client, topic}
}

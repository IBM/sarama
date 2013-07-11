package kafka

type Consumer struct {
	client *Client
	topic string
}

func NewConsumer(client *Client, topic string) *Consumer {
	return &Consumer{client, topic}
}

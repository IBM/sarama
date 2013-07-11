package kafka

type Producer struct {
	client *Client
	topic  string
}

func NewProducer(client *Client, topic string) *Producer {
	return &Producer{client, topic}
}

func (p *Producer) SendSimpleMessage(in string) error {
	partition, err := p.client.brokers.choosePartition(p.topic, randomPartitioner{})
	if err != nil {
		return err
	}

	request := newSingletonProduceRequest(p.topic, partition, newSingletonMessageSet(newMessageFromString(in)))

	return p.client.brokers.sendToPartition(p.topic, partition, request, nil)
}

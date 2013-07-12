package kafka

type Producer struct {
	client      *Client
	topic       string
	partitioner partitionChooser
}

func NewSimpleProducer(client *Client, topic string) *Producer {
	return &Producer{client, topic, RandomPartitioner{}}
}

func (p *Producer) SendMessage(key, value encoder) error {
	partitions, err := p.client.brokers.partitionsForTopic(p.topic)
	if err != nil {
		return err
	}

	partitioner := p.partitioner
	if key == nil {
		partitioner = RandomPartitioner{}
	}
	partition := partitioner.choosePartition(nil, partitions)

	msg, err := newMessage(key, value)
	if err != nil {
		return err
	}

	request := newSingletonProduceRequest(p.topic, partition, newSingletonMessageSet(msg))
	request.requiredAcks = WAIT_FOR_LOCAL

	_, err = p.client.brokers.sendToPartition(p.topic, partition, request, &produceResponse{})

	return err
}

type encodableString string

func (s encodableString) encode(pe packetEncoder) {
	pe.putRaw([]byte(s))
}

func (p *Producer) SendSimpleMessage(in string) error {
	return p.SendMessage(nil, encodableString(in))
}

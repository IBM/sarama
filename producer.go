package kafka

type Producer struct {
	client            *Client
	topic             string
	partitioner       Partitioner
	responseCondition int16
	responseTimeout   int32
}

func NewProducer(client *Client, topic string, partitioner Partitioner, responseCondition int16, responseTimeout int32) *Producer {
	return &Producer{client, topic, partitioner, responseCondition, responseTimeout}
}

func NewSimpleProducer(client *Client, topic string) *Producer {
	return NewProducer(client, topic, RandomPartitioner{}, WAIT_FOR_LOCAL, 0)
}

func (p *Producer) choosePartition(key Encoder) (int32, error) {
	partitions, err := p.client.partitions(p.topic)
	if err != nil {
		return -1, err
	}

	var partitioner Partitioner
	if key == nil {
		partitioner = RandomPartitioner{}
	} else {
		partitioner = p.partitioner
	}

	return partitions[partitioner.Partition(key, len(partitions))], nil
}

func (p *Producer) SendMessage(key, value Encoder) (*ProduceResponse, error) {
	partition, err := p.choosePartition(key)
	if err != nil {
		return nil, err
	}

	var keyBytes []byte
	var valBytes []byte

	if key != nil {
		keyBytes, err = key.Encode()
		if err != nil {
			return nil, err
		}
	}
	valBytes, err = value.Encode()
	if err != nil {
		return nil, err
	}

	broker, err := p.client.leader(p.topic, partition)
	if err != nil {
		return nil, err
	}

	request := &ProduceRequest{ResponseCondition: p.responseCondition, Timeout: p.responseTimeout}
	request.AddMessage(&p.topic, partition, &Message{Key: keyBytes, Value: valBytes})

	response, err := broker.Produce(p.client.id, request)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (p *Producer) SendSimpleMessage(in string) (*ProduceResponse, error) {
	return p.SendMessage(nil, encodableString(in))
}

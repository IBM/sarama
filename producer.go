package kafka

type Producer struct {
	client            *Client
	topic             string
	partitioner       PartitionChooser
	responseCondition int16
	responseTimeout   int32
}

func NewProducer(client *Client, topic string, partitioner PartitionChooser, responseCondition int16, responseTimeout int32) *Producer {
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

	var partitioner PartitionChooser
	if key == nil {
		partitioner = RandomPartitioner{}
	} else {
		partitioner = p.partitioner
	}

	return partitions[partitioner.ChoosePartition(key, len(partitions))], nil
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

	request := newSingletonProduceRequest(p.topic, partition, newSingletonMessageSet(&Message{Key: keyBytes, Value: valBytes}))
	request.requiredAcks = p.responseCondition
	request.timeout = p.responseTimeout

	decoder, err := broker.Send(p.client.id, request)
	if err != nil {
		return nil, err
	}
	if decoder != nil {
		return decoder.(*ProduceResponse), nil
	}

	return nil, nil
}

func (p *Producer) SendSimpleMessage(in string) (*ProduceResponse, error) {
	return p.SendMessage(nil, encodableString(in))
}

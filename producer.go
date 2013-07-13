package kafka

type Producer struct {
	*Client
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

func (p *Producer) choosePartition(key encoder) (int32, error) {
	partitions, err := p.partitions(p.topic)
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

func (p *Producer) SendMessage(key, value encoder) (*ProduceResponse, error) {
	partition, err := p.choosePartition(key)
	if err != nil {
		return nil, err
	}

	msg, err := newMessage(key, value)
	if err != nil {
		return nil, err
	}

	broker, err := p.leader(p.topic, partition)
	if err != nil {
		return nil, err
	}

	request := newSingletonProduceRequest(p.topic, partition, newSingletonMessageSet(msg))
	request.requiredAcks = p.responseCondition
	request.timeout = p.responseTimeout

	decoder, err := broker.Send(p.id, request)
	if err != nil {
		return nil, err
	}
	if decoder != nil {
		return decoder.(*ProduceResponse), nil
	}

	return nil, nil
}

type encodableString string

func (s encodableString) encode(pe packetEncoder) {
	pe.putRaw([]byte(s))
}

func (p *Producer) SendSimpleMessage(in string) (*ProduceResponse, error) {
	return p.SendMessage(nil, encodableString(in))
}

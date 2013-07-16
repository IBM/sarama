package kafka

import k "sarama/protocol"
import "time"

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
	return NewProducer(client, topic, RandomPartitioner{}, k.WAIT_FOR_LOCAL, 0)
}

func (p *Producer) SendMessage(key, value Encoder) error {
	return p.safeSendMessage(key, value, 1)
}

func (p *Producer) SendSimpleMessage(msg string) error {
	return p.safeSendMessage(nil, encodableString(msg), 1)
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

func (p *Producer) safeSendMessage(key, value Encoder, retries int) error {
	partition, err := p.choosePartition(key)
	if err != nil {
		return err
	}

	var keyBytes []byte
	var valBytes []byte

	if key != nil {
		keyBytes, err = key.Encode()
		if err != nil {
			return err
		}
	}
	valBytes, err = value.Encode()
	if err != nil {
		return err
	}

	broker, err := p.client.leader(p.topic, partition)
	if err != nil {
		return err
	}

	request := &k.ProduceRequest{ResponseCondition: p.responseCondition, Timeout: p.responseTimeout}
	request.AddMessage(p.topic, partition, &k.Message{Key: keyBytes, Value: valBytes})

	response, err := broker.Produce(p.client.id, request)
	if err != nil {
		return err
	}

	if response == nil {
		return nil
	}

	block := response.GetBlock(p.topic, partition)
	if block == nil {
		return IncompleteResponse
	}

	switch block.Err {
	case k.NO_ERROR:
		return nil
	case k.LEADER_NOT_AVAILABLE:
		if retries <= 0 {
			return block.Err
		}
		// wait for leader election to finish
		time.Sleep(250 * time.Millisecond)
		err = p.client.cache.refreshTopic(p.topic)
		if err != nil {
			return err
		}
		return p.safeSendMessage(key, value, retries-1)
	case k.UNKNOWN_TOPIC_OR_PARTITION, k.NOT_LEADER_FOR_PARTITION:
		if retries <= 0 {
			return block.Err
		}
		err = p.client.cache.refreshTopic(p.topic)
		if err != nil {
			return err
		}
		return p.safeSendMessage(key, value, retries-1)
	default:
		return block.Err
	}
}

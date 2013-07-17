package kafka

import k "sarama/protocol"
import "time"

// Producer publishes Kafka messages on a given topic. It routes messages to the correct broker, refreshing metadata as appropriate,
// and parses responses for errors. A Producer itself does not need to be closed (thus no Close method) but you still need to close
// its underlying Client.
type Producer struct {
	client      *Client
	topic       string
	partitioner Partitioner
}

// NewProducer creates a new Producer using the given client. The resulting producer will publish messages on the given topic,
// and partition messages using the given partitioner.
func NewProducer(client *Client, topic string, partitioner Partitioner) *Producer {
	p := new(Producer)
	p.client = client
	p.topic = topic
	p.partitioner = partitioner
	return p
}

// SendMessage sends a message with the given key and value. If key is nil, the partition to send to is selected randomly, otherwise it
// is selected by the Producer's Partitioner. To send strings as either key or value, see the StringEncoder type.
func (p *Producer) SendMessage(key, value Encoder) error {
	return p.safeSendMessage(key, value, true)
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

func (p *Producer) safeSendMessage(key, value Encoder, retry bool) error {
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
	switch t := err.(type) {
	case k.KError:
		if t == k.LEADER_NOT_AVAILABLE {
			time.Sleep(250 * time.Millisecond) // wait for leader election
			broker, err = p.client.leader(p.topic, partition)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	case nil:
		break
	default:
		return err
	}

	request := &k.ProduceRequest{ResponseCondition: k.WAIT_FOR_LOCAL, Timeout: 0}
	request.AddMessage(p.topic, partition, &k.Message{Key: keyBytes, Value: valBytes})

	response, err := broker.Produce(p.client.id, request)
	switch err.(type) {
	case k.EncodingError:
		return err
	case nil:
		break
	default:
		if !retry {
			return err
		}
		p.client.disconnectBroker(broker)
		return p.safeSendMessage(key, value, false)
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
		if !retry {
			return block.Err
		}
		// wait for leader election to finish
		time.Sleep(250 * time.Millisecond)
		err = p.client.refreshTopic(p.topic)
		if err != nil {
			return err
		}
		return p.safeSendMessage(key, value, false)
	case k.UNKNOWN_TOPIC_OR_PARTITION, k.NOT_LEADER_FOR_PARTITION:
		if !retry {
			return block.Err
		}
		err = p.client.refreshTopic(p.topic)
		if err != nil {
			return err
		}
		return p.safeSendMessage(key, value, false)
	default:
		return block.Err
	}
}

package sarama

// ProducerConfig is used to pass multiple configuration options to NewProducer.
type ProducerConfig struct {
	Partitioner  Partitioner      // Chooses the partition to send messages to, or randomly if this is nil.
	RequiredAcks RequiredAcks     // The level of acknowledgement reliability needed from the broker (defaults to no acknowledgement).
	Timeout      int32            // The maximum time in ms the broker will wait the receipt of the number of RequiredAcks.
	Compression  CompressionCodec // The type of compression to use on messages (defaults to no compression).
}

// Producer publishes Kafka messages on a given topic. It routes messages to the correct broker, refreshing metadata as appropriate,
// and parses responses for errors. You must call Close() on a producer to avoid leaks, it may not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
type Producer struct {
	client *Client
	topic  string
	config ProducerConfig
}

// NewProducer creates a new Producer using the given client. The resulting producer will publish messages on the given topic.
func NewProducer(client *Client, topic string, config *ProducerConfig) (*Producer, error) {
	if config == nil {
		config = new(ProducerConfig)
	}

	if config.RequiredAcks < -1 {
		return nil, ConfigurationError("Invalid RequiredAcks")
	}

	if config.Timeout < 0 {
		return nil, ConfigurationError("Invalid Timeout")
	}

	if config.Partitioner == nil {
		config.Partitioner = NewRandomPartitioner()
	}

	p := new(Producer)
	p.client = client
	p.topic = topic
	p.config = *config

	return p, nil
}

// Close shuts down the producer and flushes any messages it may have buffered. You must call this function before
// a producer object passes out of scope, as it may otherwise leak memory. You must call this before calling Close
// on the underlying client.
func (p *Producer) Close() error {
	// no-op for now, adding for consistency and so the API doesn't change when we add buffering
	// (which will require a goroutine, which will require a close method in order to flush the buffer).
	return nil
}

// SendMessage sends a message with the given key and value. The partition to send to is selected by the Producer's Partitioner.
// To send strings as either key or value, see the StringEncoder type.
func (p *Producer) SendMessage(key, value Encoder) error {
	return p.safeSendMessage(key, value, true)
}

func (p *Producer) choosePartition(key Encoder) (int32, error) {
	partitions, err := p.client.Partitions(p.topic)
	if err != nil {
		return -1, err
	}

	numPartitions := int32(len(partitions))

	choice := p.config.Partitioner.Partition(key, numPartitions)

	if choice < 0 || choice >= numPartitions {
		return -1, InvalidPartition
	}

	return partitions[choice], nil
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

	broker, err := p.client.Leader(p.topic, partition)
	if err != nil {
		return err
	}

	request := &ProduceRequest{RequiredAcks: p.config.RequiredAcks, Timeout: p.config.Timeout}
	request.AddMessage(p.topic, partition, &Message{Codec: p.config.Compression, Key: keyBytes, Value: valBytes})

	response, err := broker.Produce(p.client.id, request)
	switch err {
	case nil:
		break
	case EncodingError:
		return err
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
	case NoError:
		return nil
	case UnknownTopicOrPartition, NotLeaderForPartition, LeaderNotAvailable:
		if !retry {
			return block.Err
		}
		err = p.client.RefreshTopicMetadata(p.topic)
		if err != nil {
			return err
		}
		return p.safeSendMessage(key, value, false)
	}

	return block.Err
}

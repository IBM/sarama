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
	mp    MultiProducer
	topic string
}

// NewProducer creates a new Producer using the given client. The resulting producer will publish messages on the given topic.
func NewProducer(client *Client, topic string, config *ProducerConfig) (*Producer, error) {
	if config == nil {
		config = new(ProducerConfig)
	}

	mpc := MultiProducerConfig{
		Partitioner:    config.Partitioner,
		RequiredAcks:   config.RequiredAcks,
		Timeout:        config.Timeout,
		Compression:    config.Compression,
		MaxBufferBytes: 0, // synchronous
		MaxBufferTime:  0, // synchronous
	}
	mp, err := NewMultiProducer(client, &mpc)
	if err != nil {
		return nil, err
	}

	if topic == "" {
		return nil, ConfigurationError("Empty topic")
	}

	p := &Producer{
		topic: topic,
		mp:    *mp,
	}

	return p, nil
}

// Close shuts down the producer and flushes any messages it may have buffered. You must call this function before
// a producer object passes out of scope, as it may otherwise leak memory. You must call this before calling Close
// on the underlying client.
func (p *Producer) Close() error {
	return p.mp.Close()
}

// SendMessage sends a message with the given key and value. The partition to send to is selected by the Producer's Partitioner.
// To send strings as either key or value, see the StringEncoder type.
func (p *Producer) SendMessage(key, value Encoder) error {
	return p.mp.SendMessage(p.topic, key, value)
}

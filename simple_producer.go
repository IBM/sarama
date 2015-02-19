package sarama

import "sync"

// SimpleProducer publishes Kafka messages. It routes messages to the correct broker, refreshing metadata as appropriate,
// and parses responses for errors. You must call Close() on a producer to avoid leaks, it may not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
type SimpleProducer struct {
	producer *Producer
	wg       sync.WaitGroup
}

// NewSimpleProducer creates a new SimpleProducer using the given client  and configuration.
func NewSimpleProducer(client *Client, config *ProducerConfig) (*SimpleProducer, error) {
	if config == nil {
		config = NewProducerConfig()
	}
	config.AckSuccesses = true

	prod, err := NewProducer(client, config)

	if err != nil {
		return nil, err
	}

	sp := &SimpleProducer{producer: prod}

	sp.wg.Add(2)
	go withRecover(sp.handleSuccesses)
	go withRecover(sp.handleErrors)

	return sp, nil
}

// SendMessage produces a message to the given topic with the given key and value. To send strings as either key or value, see the StringEncoder type.
func (sp *SimpleProducer) SendMessage(topic string, key, value Encoder) error {
	expectation := make(chan error, 1)
	msg := &MessageToSend{Topic: topic, Key: key, Value: value, Metadata: expectation}
	sp.producer.Input() <- msg
	return <-expectation
}

func (sp *SimpleProducer) handleSuccesses() {
	defer sp.wg.Done()
	for msg := range sp.producer.Successes() {
		expectation := msg.Metadata.(chan error)
		expectation <- nil
	}
}

func (sp *SimpleProducer) handleErrors() {
	defer sp.wg.Done()
	for err := range sp.producer.Errors() {
		expectation := err.Msg.Metadata.(chan error)
		expectation <- err.Err
	}
}

// Close shuts down the producer and flushes any messages it may have buffered. You must call this function before
// a producer object passes out of scope, as it may otherwise leak memory. You must call this before calling Close
// on the underlying client.
func (sp *SimpleProducer) Close() error {
	sp.producer.AsyncClose()
	sp.wg.Wait()
	return nil
}

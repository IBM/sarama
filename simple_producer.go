package sarama

// SimpleProducer publishes Kafka messages. It routes messages to the correct broker, refreshing metadata as appropriate,
// and parses responses for errors. You must call Close() on a producer to avoid leaks, it may not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
type SimpleProducer struct {
	producer        *Producer
	newExpectations chan *spExpect
}

type spExpect struct {
	msg    *MessageToSend
	result chan error
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

	sp := &SimpleProducer{
		producer:        prod,
		newExpectations: make(chan *spExpect), // this must be unbuffered
	}

	go withRecover(sp.matchResponses)

	return sp, nil
}

// SendMessage produces a message to the given topic with the given key and value. To send strings as either key or value, see the StringEncoder type.
func (sp *SimpleProducer) SendMessage(topic string, key, value Encoder) error {
	msg := &MessageToSend{Topic: topic, Key: key, Value: value}
	expectation := &spExpect{msg: msg, result: make(chan error)}
	sp.newExpectations <- expectation
	sp.producer.Input() <- msg

	return <-expectation.result
}

func (sp *SimpleProducer) matchResponses() {
	newExpectations := sp.newExpectations
	unmatched := make(map[*MessageToSend]chan error)
	unmatched[nil] = nil // prevent it from emptying entirely

	for len(unmatched) > 0 {
		select {
		case expectation, ok := <-newExpectations:
			if !ok {
				delete(unmatched, nil) // let us exit when we've processed the last message
				newExpectations = nil  // prevent spinning on a closed channel until that happens
				continue
			}
			unmatched[expectation.msg] = expectation.result
		case err := <-sp.producer.Errors():
			unmatched[err.Msg] <- err.Err
			delete(unmatched, err.Msg)
		case msg := <-sp.producer.Successes():
			close(unmatched[msg])
			delete(unmatched, msg)
		}
	}
}

// Close shuts down the producer and flushes any messages it may have buffered. You must call this function before
// a producer object passes out of scope, as it may otherwise leak memory. You must call this before calling Close
// on the underlying client.
func (sp *SimpleProducer) Close() error {
	close(sp.newExpectations)
	return sp.producer.Close()
}

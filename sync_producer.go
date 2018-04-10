package sarama

import (
	"context"
	"sync"
)

// SyncProducer publishes Kafka messages, blocking until they have been acknowledged. It routes messages to the correct
// broker, refreshing metadata as appropriate, and parses responses for errors. You must call Close() on a producer
// to avoid leaks, it may not be garbage-collected automatically when it passes out of scope.
//
// The SyncProducer comes with two caveats: it will generally be less efficient than the AsyncProducer, and the actual
// durability guarantee provided when a message is acknowledged depend on the configured value of `Producer.RequiredAcks`.
// There are configurations where a message acknowledged by the SyncProducer can still sometimes be lost.
//
// For implementation reasons, the SyncProducer requires `Producer.Return.Errors` and `Producer.Return.Successes` to
// be set to true in its configuration.
type SyncProducer interface {

	// SendMessage produces a given message, and returns only when it either has
	// succeeded or failed to produce. It will return the partition and the offset
	// of the produced message, or an error if the message failed to produce.
	SendMessage(msg *ProducerMessage) (partition int32, offset int64, err error)

	// SendMessagesContext produces a given message, and returns when it has either succeed
	// or failed to produce or if the context is done. It will return the partition and the offset
	// of the produced message, or an error if the message failed to produce.
	SendMessageContext(ctx context.Context, msg *ProducerMessage) (partition int32, offset int64, err error)

	// SendMessages produces a given set of messages, and returns only when all
	// messages in the set have either succeeded or failed. Note that messages
	// can succeed and fail individually; if some succeed and some fail,
	// SendMessages will return an error.
	// If the context is canceled before all messages have been enqueued, some might
	// never be enqueued.
	SendMessages(msgs []*ProducerMessage) error

	// SendMessages produces a given set of messages, and returns when all
	// messages in the set have either succeeded or failed or the context is done.
	// Note that messages can succeed and fail individually; if some succeed and some fail,
	// SendMessages will return an error.
	SendMessagesContext(ctx context.Context, msgs []*ProducerMessage) error

	// Close shuts down the producer and waits for any buffered messages to be
	// flushed. You must call this function before a producer object passes out of
	// scope, as it may otherwise leak memory. You must call this before calling
	// Close on the underlying client.
	Close() error
}

type syncProducer struct {
	producer *asyncProducer
	wg       sync.WaitGroup
}

// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
func NewSyncProducer(addrs []string, config *Config) (SyncProducer, error) {
	if config == nil {
		config = NewConfig()
		config.Producer.Return.Successes = true
	}

	if err := verifyProducerConfig(config); err != nil {
		return nil, err
	}

	p, err := NewAsyncProducer(addrs, config)
	if err != nil {
		return nil, err
	}
	return newSyncProducerFromAsyncProducer(p.(*asyncProducer)), nil
}

// NewSyncProducerFromClient creates a new SyncProducer using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this producer.
func NewSyncProducerFromClient(client Client) (SyncProducer, error) {
	if err := verifyProducerConfig(client.Config()); err != nil {
		return nil, err
	}

	p, err := NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}
	return newSyncProducerFromAsyncProducer(p.(*asyncProducer)), nil
}

func newSyncProducerFromAsyncProducer(p *asyncProducer) *syncProducer {
	sp := &syncProducer{producer: p}

	sp.wg.Add(2)
	go withRecover(sp.handleSuccesses)
	go withRecover(sp.handleErrors)

	return sp
}

func verifyProducerConfig(config *Config) error {
	if !config.Producer.Return.Errors {
		return ConfigurationError("Producer.Return.Errors must be true to be used in a SyncProducer")
	}
	if !config.Producer.Return.Successes {
		return ConfigurationError("Producer.Return.Successes must be true to be used in a SyncProducer")
	}
	return nil
}

func (sp *syncProducer) SendMessage(
	msg *ProducerMessage,
) (partition int32, offset int64, err error) {
	return sp.SendMessageContext(context.Background(), msg)
}

func (sp *syncProducer) SendMessageContext(
	ctx context.Context,
	msg *ProducerMessage,
) (partition int32, offset int64, err error) {
	oldMetadata := msg.Metadata
	defer func() {
		msg.Metadata = oldMetadata
	}()

	expectation := make(chan *ProducerError, 1)
	msg.Metadata = expectation

	select {
	case <-ctx.Done():
		return -1, -1, ctx.Err()
	case sp.producer.Input() <- msg:
	}

	select {
	case <-ctx.Done():
		return -1, -1, ctx.Err()
	case err := <-expectation:
		if err != nil {
			return -1, -1, err.Err
		}
	}

	return msg.Partition, msg.Offset, nil
}

func (sp *syncProducer) SendMessages(
	msgs []*ProducerMessage,
) error {
	return sp.SendMessagesContext(context.Background(), msgs)
}

func (sp *syncProducer) SendMessagesContext(
	ctx context.Context,
	msgs []*ProducerMessage,
) error {
	savedMetadata := make([]interface{}, len(msgs))
	for i := range msgs {
		savedMetadata[i] = msgs[i].Metadata
	}
	defer func() {
		for i := range msgs {
			msgs[i].Metadata = savedMetadata[i]
		}
	}()

	var err error
	expectations := make(chan chan *ProducerError, len(msgs))
	go func() {
		defer close(expectations)
		for _, msg := range msgs {
			expectation := make(chan *ProducerError, 1)
			msg.Metadata = expectation

			select {
			case <-ctx.Done():
				// Abort early and don't enqueue remaining messages (no one will be waiting for them).
				err = ctx.Err()
				return
			case sp.producer.Input() <- msg:
			}

			expectations <- expectation
		}
	}()

	var errors ProducerErrors
	for expectation := range expectations {
		// Wait for context or an expectation
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case producerErr := <-expectation:
			if producerErr != nil {
				errors = append(errors, producerErr)
			}
		}
	}

	if err != nil {
		return err
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

func (sp *syncProducer) handleSuccesses() {
	defer sp.wg.Done()
	for msg := range sp.producer.Successes() {
		expectation := msg.Metadata.(chan *ProducerError)
		expectation <- nil
	}
}

func (sp *syncProducer) handleErrors() {
	defer sp.wg.Done()
	for err := range sp.producer.Errors() {
		expectation := err.Msg.Metadata.(chan *ProducerError)
		expectation <- err
	}
}

func (sp *syncProducer) Close() error {
	sp.producer.AsyncClose()
	sp.wg.Wait()
	return nil
}

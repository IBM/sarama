package sarama

import "sync"

// SyncProducer publishes Kafka messages. It routes messages to the correct broker, refreshing metadata as appropriate,
// and parses responses for errors. You must call Close() on a producer to avoid leaks, it may not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
type SyncProducer interface {

	// SendMessage produces a given message, and returns when it has succeeded or failed.
	// It will set the OPartition and Offset fields for a successfully produced message.
	SendMessage(*ProducerMessage) error

	// Close shuts down the producer and flushes any messages it may have buffered. You must call this function before
	// a producer object passes out of scope, as it may otherwise leak memory. You must call this before calling Close
	// on the underlying client.
	Close() error
}

type syncProducer struct {
	producer *asyncProducer
	wg       sync.WaitGroup
}

// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
func NewSyncProducer(addrs []string, config *Config) (SyncProducer, error) {
	p, err := NewAsyncProducer(addrs, config)
	if err != nil {
		return nil, err
	}
	return newSyncProducerFromAsyncProducer(p.(*asyncProducer)), nil
}

// NewSyncProducerFromClient creates a new SyncProducer using the given client.
func NewSyncProducerFromClient(client Client) (SyncProducer, error) {
	p, err := NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}
	return newSyncProducerFromAsyncProducer(p.(*asyncProducer)), nil
}

func newSyncProducerFromAsyncProducer(p *asyncProducer) *syncProducer {
	p.conf.Producer.Return.Successes = true
	p.conf.Producer.Return.Errors = true
	sp := &syncProducer{producer: p}

	sp.wg.Add(2)
	go withRecover(sp.handleSuccesses)
	go withRecover(sp.handleErrors)

	return sp
}

func (sp *syncProducer) SendMessage(msg *ProducerMessage) error {
	oldMetadata := msg.Metadata
	defer func() {
		msg.Metadata = oldMetadata
	}()

	expectation := make(chan error, 1)
	msg.Metadata = expectation
	sp.producer.Input() <- msg
	return <-expectation
}

func (sp *syncProducer) handleSuccesses() {
	defer sp.wg.Done()
	for msg := range sp.producer.Successes() {
		expectation := msg.Metadata.(chan error)
		expectation <- nil
	}
}

func (sp *syncProducer) handleErrors() {
	defer sp.wg.Done()
	for err := range sp.producer.Errors() {
		expectation := err.Msg.Metadata.(chan error)
		expectation <- err.Err
	}
}

func (sp *syncProducer) Close() error {
	sp.producer.AsyncClose()
	sp.wg.Wait()
	return nil
}

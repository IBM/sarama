package sarama

import (
	"context"
	"sync"
)

var expectationsPool = sync.Pool{
	New: func() any {
		return make(chan *ProducerError, 1)
	},
}

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

	// SendMessages produces a given set of messages, and returns only when all
	// messages in the set have either succeeded or failed. Note that messages
	// can succeed and fail individually; if some succeed and some fail,
	// SendMessages will return an error.
	SendMessages(msgs []*ProducerMessage) error

	// SendMessageContext is like SendMessage, but returns ctx.Err() if ctx is
	// done before the message has either succeeded or failed. The message is
	// then still owned by the producer and must not be reused.
	SendMessageContext(ctx context.Context, msg *ProducerMessage) (partition int32, offset int64, err error)

	// SendMessagesContext is like SendMessages, but returns ctx.Err() if ctx is
	// done before all messages have either succeeded or failed. Messages without
	// a result are then still owned by the producer and must not be reused.
	SendMessagesContext(ctx context.Context, msgs []*ProducerMessage) error

	// Close shuts down the producer; you must call this function before a producer
	// object passes out of scope, as it may otherwise leak memory.
	// You must call this before calling Close on the underlying client.
	Close() error

	// TxnStatus return current producer transaction status.
	TxnStatus() ProducerTxnStatusFlag

	// IsTransactional return true when current producer is transactional.
	IsTransactional() bool

	// BeginTxn mark current transaction as ready.
	BeginTxn() error

	// CommitTxn commit current transaction.
	CommitTxn() error

	// AbortTxn abort current transaction.
	AbortTxn() error

	// AddOffsetsToTxn add associated offsets to current transaction.
	AddOffsetsToTxn(offsets map[string][]*PartitionOffsetMetadata, groupId string) error

	// AddOffsetsToTxnWithGroupMetadata adds associated offsets to the current
	// transaction, carrying the consumer group member metadata so the broker
	// can fence stale members (KIP-447).
	AddOffsetsToTxnWithGroupMetadata(offsets map[string][]*PartitionOffsetMetadata, groupMetadata *ConsumerGroupMetadata) error

	// AddMessageToTxn add message offsets to current transaction.
	AddMessageToTxn(msg *ConsumerMessage, groupId string, metadata *string) error

	// AddMessageToTxnWithGroupMetadata adds the message offset to the current
	// transaction, carrying the consumer group member metadata so the broker
	// can fence stale members (KIP-447).
	AddMessageToTxnWithGroupMetadata(msg *ConsumerMessage, groupMetadata *ConsumerGroupMetadata, metadata *string) error
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

func (sp *syncProducer) SendMessage(msg *ProducerMessage) (partition int32, offset int64, err error) {
	return sp.SendMessageContext(context.Background(), msg)
}

func (sp *syncProducer) SendMessageContext(ctx context.Context, msg *ProducerMessage) (partition int32, offset int64, err error) {
	if err := ctx.Err(); err != nil {
		return -1, -1, err
	}

	expectation := expectationsPool.Get().(chan *ProducerError)
	msg.expectation = expectation
	select {
	case sp.producer.Input() <- msg:
	case <-ctx.Done():
		msg.expectation = nil
		expectationsPool.Put(expectation)
		return -1, -1, ctx.Err()
	}

	var pErr *ProducerError
	select {
	case pErr = <-expectation:
	case <-ctx.Done():
		// the producer still has the expectation, so it can't go back in the pool
		return -1, -1, ctx.Err()
	}
	msg.expectation = nil
	expectationsPool.Put(expectation)
	if pErr != nil {
		return -1, -1, pErr.Err
	}

	return msg.Partition, msg.Offset, nil
}

func (sp *syncProducer) SendMessages(msgs []*ProducerMessage) error {
	return sp.SendMessagesContext(context.Background(), msgs)
}

func (sp *syncProducer) SendMessagesContext(ctx context.Context, msgs []*ProducerMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	indices := make(chan int, len(msgs))
	go func() {
		defer close(indices)
		for i, msg := range msgs {
			expectation := expectationsPool.Get().(chan *ProducerError)
			msg.expectation = expectation
			select {
			case sp.producer.Input() <- msg:
			case <-ctx.Done():
				msg.expectation = nil
				expectationsPool.Put(expectation)
				return
			}
			indices <- i
		}
	}()

	var errors ProducerErrors
	var results int
	for i := range indices {
		expectation := msgs[i].expectation
		var pErr *ProducerError
		select {
		case pErr = <-expectation:
		case <-ctx.Done():
			// wait for the sender to stop so nothing reaches the producer after we return
			for range indices {
			}
			return ctx.Err()
		}
		msgs[i].expectation = nil
		expectationsPool.Put(expectation)
		results++
		if pErr != nil {
			errors = append(errors, pErr)
		}
	}
	if results < len(msgs) {
		return ctx.Err()
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

func (sp *syncProducer) handleSuccesses() {
	defer sp.wg.Done()
	for msg := range sp.producer.Successes() {
		expectation := msg.expectation
		expectation <- nil
	}
}

func (sp *syncProducer) handleErrors() {
	defer sp.wg.Done()
	for err := range sp.producer.Errors() {
		expectation := err.Msg.expectation
		expectation <- err
	}
}

func (sp *syncProducer) Close() error {
	sp.producer.AsyncClose()
	sp.wg.Wait()
	return nil
}

func (sp *syncProducer) IsTransactional() bool {
	return sp.producer.IsTransactional()
}

func (sp *syncProducer) BeginTxn() error {
	return sp.producer.BeginTxn()
}

func (sp *syncProducer) CommitTxn() error {
	return sp.producer.CommitTxn()
}

func (sp *syncProducer) AbortTxn() error {
	return sp.producer.AbortTxn()
}

func (sp *syncProducer) AddOffsetsToTxn(offsets map[string][]*PartitionOffsetMetadata, groupId string) error {
	return sp.producer.AddOffsetsToTxn(offsets, groupId)
}

func (sp *syncProducer) AddOffsetsToTxnWithGroupMetadata(offsets map[string][]*PartitionOffsetMetadata, groupMetadata *ConsumerGroupMetadata) error {
	return sp.producer.AddOffsetsToTxnWithGroupMetadata(offsets, groupMetadata)
}

func (sp *syncProducer) AddMessageToTxn(msg *ConsumerMessage, groupId string, metadata *string) error {
	return sp.producer.AddMessageToTxn(msg, groupId, metadata)
}

func (sp *syncProducer) AddMessageToTxnWithGroupMetadata(msg *ConsumerMessage, groupMetadata *ConsumerGroupMetadata, metadata *string) error {
	return sp.producer.AddMessageToTxnWithGroupMetadata(msg, groupMetadata, metadata)
}

func (p *syncProducer) TxnStatus() ProducerTxnStatusFlag {
	return p.producer.TxnStatus()
}

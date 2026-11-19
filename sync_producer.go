package sarama

import (
	"context"
	"errors"
	"fmt"
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

// SyncProducerContext is a SyncProducer whose sends can be bounded by a
// context. The producers returned by NewSyncProducer and
// NewSyncProducerFromClient implement it.
//
// If a context ends after a message is submitted to the producer but before
// its result arrives, its outcome is unknown. It may still be delivered and
// must not be reused. Close waits for submitted messages, even after their
// send calls return; if a result never arrives, Close does not return.
type SyncProducerContext interface {
	SyncProducer

	// SendMessageContext is like SendMessage, but returns an error wrapping
	// ctx.Err() if ctx is done before the message has either succeeded or
	// failed. If the message was not submitted to the producer, the error
	// also wraps ErrMessageNotSubmitted and the message can be sent again.
	SendMessageContext(ctx context.Context, msg *ProducerMessage) (partition int32, offset int64, err error)

	// SendMessagesContext is like SendMessages, but returns once ctx is done
	// even if some messages have neither succeeded nor failed. The error then
	// wraps ctx.Err() and a ProducerErrors holding the messages that failed
	// and the messages that have no result, whose errors wrap ctx.Err().
	// Errors for messages that were not submitted to the producer also wrap
	// ErrMessageNotSubmitted, and those messages can be sent again. Messages
	// not listed in it succeeded.
	SendMessagesContext(ctx context.Context, msgs []*ProducerMessage) error
}

var _ SyncProducerContext = (*syncProducer)(nil)

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
		return -1, -1, notSubmittedError(err)
	}

	expectation := expectationsPool.Get().(chan *ProducerError)
	msg.expectation = expectation
	select {
	case sp.producer.Input() <- msg:
	case <-ctx.Done():
		// the producer never received the message, so nothing else holds the expectation
		msg.expectation = nil
		expectationsPool.Put(expectation)
		return -1, -1, notSubmittedError(ctx.Err())
	}

	select {
	case pErr := <-expectation:
		msg.expectation = nil
		expectationsPool.Put(expectation)
		if pErr != nil {
			return -1, -1, pErr.Err
		}
		return msg.Partition, msg.Offset, nil
	case <-ctx.Done():
		// the producer still has the expectation, so it can't go back in the pool
		return -1, -1, ctx.Err()
	}
}

func (sp *syncProducer) SendMessages(msgs []*ProducerMessage) error {
	return sp.SendMessagesContext(context.Background(), msgs)
}

func (sp *syncProducer) SendMessagesContext(ctx context.Context, msgs []*ProducerMessage) error {
	// each result channel holds one outcome, so sending does not depend on collecting results
	sent := 0
send:
	for _, msg := range msgs {
		// select picks at random when both cases are ready, so stop before offering another message
		if ctx.Err() != nil {
			break
		}
		expectation := expectationsPool.Get().(chan *ProducerError)
		msg.expectation = expectation
		select {
		case sp.producer.Input() <- msg:
			sent++
		case <-ctx.Done():
			// the producer never received the message, so nothing else holds the expectation
			msg.expectation = nil
			expectationsPool.Put(expectation)
			break send
		}
	}

	var pErrs ProducerErrors
	unfinished := len(msgs) - sent
	for _, msg := range msgs[:sent] {
		pErr, ok := waitResult(ctx, msg)
		if !ok {
			unfinished++
			pErr = &ProducerError{Msg: msg, Err: ctx.Err()}
		}
		if pErr != nil {
			pErrs = append(pErrs, pErr)
		}
	}
	for _, msg := range msgs[sent:] {
		pErrs = append(pErrs, &ProducerError{Msg: msg, Err: notSubmittedError(ctx.Err())})
	}

	if unfinished > 0 {
		return errors.Join(ctx.Err(), pErrs)
	}
	if len(pErrs) > 0 {
		return pErrs
	}
	return nil
}

// waitResult reports false if ctx ends before msg's result arrives
func waitResult(ctx context.Context, msg *ProducerMessage) (*ProducerError, bool) {
	var pErr *ProducerError
	select {
	case pErr = <-msg.expectation:
	case <-ctx.Done():
		// select picks at random when both cases are ready, so take a result that has already arrived
		select {
		case pErr = <-msg.expectation:
		default:
			// the producer may still write a result, so the channel must not be reused
			return nil, false
		}
	}
	expectationsPool.Put(msg.expectation)
	msg.expectation = nil
	return pErr, true
}

func notSubmittedError(err error) error {
	return fmt.Errorf("%w: %w", ErrMessageNotSubmitted, err)
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

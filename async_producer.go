package sarama

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/eapache/go-resiliency/breaker"
	"github.com/eapache/queue"
	"github.com/rcrowley/go-metrics"
)

// AsyncProducer publishes Kafka messages using a non-blocking API. It routes messages
// to the correct broker for the provided topic-partition, refreshing metadata as appropriate,
// and parses responses for errors. You must read from the Errors() channel or the
// producer will deadlock. You must call Close() or AsyncClose() on a producer to avoid
// leaks and message lost: it will not be garbage-collected automatically when it passes
// out of scope and buffered messages may not be flushed.
type AsyncProducer interface {
	// AsyncClose triggers a shutdown of the producer. The shutdown has completed
	// when both the Errors and Successes channels have been closed. When calling
	// AsyncClose, you *must* continue to read from those channels in order to
	// drain the results of any messages in flight.
	AsyncClose()

	// Close shuts down the producer and waits for any buffered messages to be
	// flushed. You must call this function before a producer object passes out of
	// scope, as it may otherwise leak memory. You must call this before process
	// shutting down, or you may lose messages. You must call this before calling
	// Close on the underlying client.
	Close() error

	// Input is the input channel for the user to write messages to that they
	// wish to send.
	Input() chan<- *ProducerMessage

	// Successes is the success output channel back to the user when Return.Successes is
	// enabled. If Return.Successes is true, you MUST read from this channel or the
	// Producer will deadlock. It is suggested that you send and read messages
	// together in a single select statement.
	Successes() <-chan *ProducerMessage

	// Errors is the error output channel back to the user. You MUST read from this
	// channel or the Producer will deadlock when the channel is full. Alternatively,
	// you can set Producer.Return.Errors in your config to false, which prevents
	// errors to be returned.
	Errors() <-chan *ProducerError

	// IsTransactional return true when current producer is transactional.
	IsTransactional() bool

	// TxnStatus return current producer transaction status.
	TxnStatus() ProducerTxnStatusFlag

	// BeginTxn mark current transaction as ready.
	BeginTxn() error

	// CommitTxn commit current transaction.
	CommitTxn() error

	// AbortTxn abort current transaction.
	AbortTxn() error

	// AddOffsetsToTxn add associated offsets to current transaction.
	AddOffsetsToTxn(offsets map[string][]*PartitionOffsetMetadata, groupId string) error

	// AddMessageToTxn add message offsets to current transaction.
	AddMessageToTxn(msg *ConsumerMessage, groupId string, metadata *string) error
}

type asyncProducer struct {
	client Client
	conf   *Config

	errors                    chan *ProducerError
	input, successes, retries chan *ProducerMessage
	inFlight                  sync.WaitGroup

	brokers    map[*Broker]*brokerProducer
	brokerRefs map[*brokerProducer]int
	brokerLock sync.Mutex

	txnmgr *transactionManager
	txLock sync.Mutex

	metricsRegistry metrics.Registry
}

// NewAsyncProducer creates a new AsyncProducer using the given broker addresses and configuration.
func NewAsyncProducer(addrs []string, conf *Config) (AsyncProducer, error) {
	client, err := NewClient(addrs, conf)
	if err != nil {
		return nil, err
	}
	return newAsyncProducer(client)
}

// NewAsyncProducerFromClient creates a new Producer using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this producer.
func NewAsyncProducerFromClient(client Client) (AsyncProducer, error) {
	// For clients passed in by the client, ensure we don't
	// call Close() on it.
	cli := &nopCloserClient{client}
	return newAsyncProducer(cli)
}

func newAsyncProducer(client Client) (AsyncProducer, error) {
	// Check that we are not dealing with a closed Client before processing any other arguments
	if client.Closed() {
		return nil, ErrClosedClient
	}

	txnmgr, err := newTransactionManager(client.Config(), client)
	if err != nil {
		return nil, err
	}

	p := &asyncProducer{
		client:          client,
		conf:            client.Config(),
		errors:          make(chan *ProducerError),
		input:           make(chan *ProducerMessage),
		successes:       make(chan *ProducerMessage),
		retries:         make(chan *ProducerMessage),
		brokers:         make(map[*Broker]*brokerProducer),
		brokerRefs:      make(map[*brokerProducer]int),
		txnmgr:          txnmgr,
		metricsRegistry: newCleanupRegistry(client.Config().MetricRegistry),
	}

	// launch our singleton dispatchers
	go withRecover(p.dispatcher)
	go withRecover(p.retryHandler)

	return p, nil
}

type flagSet int8

const (
	syn       flagSet = 1 << iota // first message from partitionProducer to brokerProducer
	fin                           // final message from partitionProducer to brokerProducer and back
	shutdown                      // start the shutdown process
	endtxn                        // endtxn
	committxn                     // endtxn
	aborttxn                      // endtxn
)

// ProducerMessage is the collection of elements passed to the Producer in order to send a message.
type ProducerMessage struct {
	Topic string // The Kafka topic for this message.
	// The partitioning key for this message. Pre-existing Encoders include
	// StringEncoder and ByteEncoder.
	Key Encoder
	// The actual message to store in Kafka. Pre-existing Encoders include
	// StringEncoder and ByteEncoder.
	Value Encoder

	// The headers are key-value pairs that are transparently passed
	// by Kafka between producers and consumers.
	Headers []RecordHeader

	// This field is used to hold arbitrary data you wish to include so it
	// will be available when receiving on the Successes and Errors channels.
	// Sarama completely ignores this field and is only to be used for
	// pass-through data.
	Metadata interface{}

	// Below this point are filled in by the producer as the message is processed

	// Offset is the offset of the message stored on the broker. This is only
	// guaranteed to be defined if the message was successfully delivered and
	// RequiredAcks is not NoResponse.
	Offset int64
	// Partition is the partition that the message was sent to. This is only
	// guaranteed to be defined if the message was successfully delivered.
	Partition int32
	// Timestamp can vary in behavior depending on broker configuration, being
	// in either one of the CreateTime or LogAppendTime modes (default CreateTime),
	// and requiring version at least 0.10.0.
	//
	// When configured to CreateTime, the timestamp is specified by the producer
	// either by explicitly setting this field, or when the message is added
	// to a produce set.
	//
	// When configured to LogAppendTime, the timestamp assigned to the message
	// by the broker. This is only guaranteed to be defined if the message was
	// successfully delivered and RequiredAcks is not NoResponse.
	Timestamp time.Time

	retries        int
	flags          flagSet
	expectation    chan *ProducerError
	sequenceNumber int32
	producerEpoch  int16
	hasSequence    bool
	hasBeenBatched bool
}

const producerMessageOverhead = 26 // the metadata overhead of CRC, flags, etc.

func (m *ProducerMessage) ByteSize(version int) int {
	var size int
	if version >= 2 {
		size = maximumRecordOverhead
		for _, h := range m.Headers {
			size += len(h.Key) + len(h.Value) + 2*binary.MaxVarintLen32
		}
	} else {
		size = producerMessageOverhead
	}
	if m.Key != nil {
		size += m.Key.Length()
	}
	if m.Value != nil {
		size += m.Value.Length()
	}
	return size
}

func (m *ProducerMessage) clear() {
	m.flags = 0
	m.retries = 0
	m.sequenceNumber = 0
	m.producerEpoch = 0
	m.hasSequence = false
}

// ProducerError is the type of error generated when the producer fails to deliver a message.
// It contains the original ProducerMessage as well as the actual error value.
type ProducerError struct {
	Msg *ProducerMessage
	Err error
}

func (pe ProducerError) Error() string {
	return fmt.Sprintf("kafka: Failed to produce message to topic %s: %s", pe.Msg.Topic, pe.Err)
}

func (pe ProducerError) Unwrap() error {
	return pe.Err
}

// ProducerErrors is a type that wraps a batch of "ProducerError"s and implements the Error interface.
// It can be returned from the Producer's Close method to avoid the need to manually drain the Errors channel
// when closing a producer.
type ProducerErrors []*ProducerError

func (pe ProducerErrors) Error() string {
	return fmt.Sprintf("kafka: Failed to deliver %d messages.", len(pe))
}

func (p *asyncProducer) IsTransactional() bool {
	return p.txnmgr.isTransactional()
}

func (p *asyncProducer) AddMessageToTxn(msg *ConsumerMessage, groupId string, metadata *string) error {
	offsets := make(map[string][]*PartitionOffsetMetadata)
	offsets[msg.Topic] = []*PartitionOffsetMetadata{
		{
			Partition: msg.Partition,
			Offset:    msg.Offset + 1,
			Metadata:  metadata,
		},
	}
	return p.AddOffsetsToTxn(offsets, groupId)
}

func (p *asyncProducer) AddOffsetsToTxn(offsets map[string][]*PartitionOffsetMetadata, groupId string) error {
	p.txLock.Lock()
	defer p.txLock.Unlock()

	if !p.IsTransactional() {
		DebugLogger.Printf("producer/txnmgr [%s] attempt to call AddOffsetsToTxn on a non-transactional producer\n", p.txnmgr.transactionalID)
		return ErrNonTransactedProducer
	}

	DebugLogger.Printf("producer/txnmgr [%s] add offsets to transaction\n", p.txnmgr.transactionalID)
	return p.txnmgr.addOffsetsToTxn(offsets, groupId)
}

func (p *asyncProducer) TxnStatus() ProducerTxnStatusFlag {
	return p.txnmgr.currentTxnStatus()
}

func (p *asyncProducer) BeginTxn() error {
	p.txLock.Lock()
	defer p.txLock.Unlock()

	if !p.IsTransactional() {
		DebugLogger.Println("producer/txnmgr attempt to call BeginTxn on a non-transactional producer")
		return ErrNonTransactedProducer
	}

	return p.txnmgr.transitionTo(ProducerTxnFlagInTransaction, nil)
}

func (p *asyncProducer) CommitTxn() error {
	p.txLock.Lock()
	defer p.txLock.Unlock()

	if !p.IsTransactional() {
		DebugLogger.Printf("producer/txnmgr [%s] attempt to call CommitTxn on a non-transactional producer\n", p.txnmgr.transactionalID)
		return ErrNonTransactedProducer
	}

	DebugLogger.Printf("producer/txnmgr [%s] committing transaction\n", p.txnmgr.transactionalID)
	err := p.finishTransaction(true)
	if err != nil {
		return err
	}
	DebugLogger.Printf("producer/txnmgr [%s] transaction committed\n", p.txnmgr.transactionalID)
	return nil
}

func (p *asyncProducer) AbortTxn() error {
	p.txLock.Lock()
	defer p.txLock.Unlock()

	if !p.IsTransactional() {
		DebugLogger.Printf("producer/txnmgr [%s] attempt to call AbortTxn on a non-transactional producer\n", p.txnmgr.transactionalID)
		return ErrNonTransactedProducer
	}
	DebugLogger.Printf("producer/txnmgr [%s] aborting transaction\n", p.txnmgr.transactionalID)
	err := p.finishTransaction(false)
	if err != nil {
		return err
	}
	DebugLogger.Printf("producer/txnmgr [%s] transaction aborted\n", p.txnmgr.transactionalID)
	return nil
}

func (p *asyncProducer) finishTransaction(commit bool) error {
	p.inFlight.Add(1)
	if commit {
		p.input <- &ProducerMessage{flags: endtxn | committxn}
	} else {
		p.input <- &ProducerMessage{flags: endtxn | aborttxn}
	}
	p.inFlight.Wait()
	return p.txnmgr.finishTransaction(commit)
}

func (p *asyncProducer) Errors() <-chan *ProducerError {
	return p.errors
}

func (p *asyncProducer) Successes() <-chan *ProducerMessage {
	return p.successes
}

func (p *asyncProducer) Input() chan<- *ProducerMessage {
	return p.input
}

func (p *asyncProducer) Close() error {
	p.AsyncClose()

	if p.conf.Producer.Return.Successes {
		go withRecover(func() {
			for range p.successes {
			}
		})
	}

	var pErrs ProducerErrors
	if p.conf.Producer.Return.Errors {
		for event := range p.errors {
			pErrs = append(pErrs, event)
		}
	} else {
		<-p.errors
	}

	if len(pErrs) > 0 {
		return pErrs
	}
	return nil
}

func (p *asyncProducer) AsyncClose() {
	go withRecover(p.shutdown)
}

// singleton
// dispatches messages by topic
func (p *asyncProducer) dispatcher() {
	handlers := make(map[string]chan<- *ProducerMessage)
	shuttingDown := false

	for msg := range p.input {
		if msg == nil {
			Logger.Println("Something tried to send a nil message, it was ignored.")
			continue
		}

		if msg.flags&endtxn != 0 {
			var err error
			if msg.flags&committxn != 0 {
				err = p.txnmgr.transitionTo(ProducerTxnFlagEndTransaction|ProducerTxnFlagCommittingTransaction, nil)
			} else {
				err = p.txnmgr.transitionTo(ProducerTxnFlagEndTransaction|ProducerTxnFlagAbortingTransaction, nil)
			}
			if err != nil {
				Logger.Printf("producer/txnmgr unable to end transaction %s", err)
			}
			p.inFlight.Done()
			continue
		}

		if msg.flags&shutdown != 0 {
			shuttingDown = true
			p.inFlight.Done()
			continue
		}

		if msg.retries == 0 {
			if shuttingDown {
				// we can't just call returnError here because that decrements the wait group,
				// which hasn't been incremented yet for this message, and shouldn't be
				pErr := &ProducerError{Msg: msg, Err: ErrShuttingDown}
				if p.conf.Producer.Return.Errors {
					p.errors <- pErr
				} else {
					Logger.Println(pErr)
				}
				continue
			}
			p.inFlight.Add(1)
			// Ignore retried msg, there are already in txn.
			// Can't produce new record when transaction is not started.
			if p.IsTransactional() && p.txnmgr.currentTxnStatus()&ProducerTxnFlagInTransaction == 0 {
				Logger.Printf("attempt to send message when transaction is not started or is in ending state, got %d, expect %d\n", p.txnmgr.currentTxnStatus(), ProducerTxnFlagInTransaction)
				p.returnError(msg, ErrTransactionNotReady)
				continue
			}
		}

		for _, interceptor := range p.conf.Producer.Interceptors {
			msg.safelyApplyInterceptor(interceptor)
		}

		version := 1
		if p.conf.Version.IsAtLeast(V0_11_0_0) {
			version = 2
		} else if msg.Headers != nil {
			p.returnError(msg, ConfigurationError("Producing headers requires Kafka at least v0.11"))
			continue
		}

		size := msg.ByteSize(version)
		if size > p.conf.Producer.MaxMessageBytes {
			p.returnError(msg, ConfigurationError(fmt.Sprintf("Attempt to produce message larger than configured Producer.MaxMessageBytes: %d > %d", size, p.conf.Producer.MaxMessageBytes)))
			continue
		}

		handler := handlers[msg.Topic]
		if handler == nil {
			handler = p.newTopicProducer(msg.Topic)
			handlers[msg.Topic] = handler
		}

		handler <- msg
	}

	for _, handler := range handlers {
		close(handler)
	}
}

// one per topic
// partitions messages, then dispatches them by partition
type topicProducer struct {
	parent *asyncProducer
	topic  string
	input  <-chan *ProducerMessage

	breaker     *breaker.Breaker
	handlers    map[int32]chan<- *ProducerMessage
	partitioner Partitioner
}

func (p *asyncProducer) newTopicProducer(topic string) chan<- *ProducerMessage {
	input := make(chan *ProducerMessage, p.conf.ChannelBufferSize)
	tp := &topicProducer{
		parent:      p,
		topic:       topic,
		input:       input,
		breaker:     breaker.New(3, 1, 10*time.Second),
		handlers:    make(map[int32]chan<- *ProducerMessage),
		partitioner: p.conf.Producer.Partitioner(topic),
	}
	go withRecover(tp.dispatch)
	return input
}

func (tp *topicProducer) dispatch() {
	for msg := range tp.input {
		if msg.retries == 0 {
			if err := tp.partitionMessage(msg); err != nil {
				tp.parent.returnError(msg, err)
				continue
			}
		}

		handler := tp.handlers[msg.Partition]
		if handler == nil {
			handler = tp.parent.newPartitionProducer(msg.Topic, msg.Partition)
			tp.handlers[msg.Partition] = handler
		}

		handler <- msg
	}

	for _, handler := range tp.handlers {
		close(handler)
	}
}

func (tp *topicProducer) partitionMessage(msg *ProducerMessage) error {
	var partitions []int32

	err := tp.breaker.Run(func() (err error) {
		requiresConsistency := false
		if ep, ok := tp.partitioner.(DynamicConsistencyPartitioner); ok {
			requiresConsistency = ep.MessageRequiresConsistency(msg)
		} else {
			requiresConsistency = tp.partitioner.RequiresConsistency()
		}

		if requiresConsistency {
			partitions, err = tp.parent.client.Partitions(msg.Topic)
		} else {
			partitions, err = tp.parent.client.WritablePartitions(msg.Topic)
		}
		return
	})
	if err != nil {
		return err
	}

	numPartitions := int32(len(partitions))

	if numPartitions == 0 {
		return ErrLeaderNotAvailable
	}

	choice, err := tp.partitioner.Partition(msg, numPartitions)

	if err != nil {
		return err
	} else if choice < 0 || choice >= numPartitions {
		return ErrInvalidPartition
	}

	msg.Partition = partitions[choice]

	return nil
}

// one per partition per topic
// dispatches messages to the appropriate broker
// also responsible for maintaining message order during retries
type partitionProducer struct {
	parent    *asyncProducer
	topic     string
	partition int32
	input     <-chan *ProducerMessage

	leader         *Broker
	breaker        *breaker.Breaker
	brokerProducer *brokerProducer

	// highWatermark tracks the "current" retry level, which is the only one where we actually let messages through,
	// all other messages get buffered in retryState[msg.retries].buf to preserve ordering
	// retryState[msg.retries].expectChaser simply tracks whether we've seen a fin message for a given level (and
	// therefore whether our buffer is complete and safe to flush)
	highWatermark int
	retryState    []partitionRetryState
}

type partitionRetryState struct {
	buf          []*ProducerMessage
	expectChaser bool
}

func (p *asyncProducer) newPartitionProducer(topic string, partition int32) chan<- *ProducerMessage {
	input := make(chan *ProducerMessage, p.conf.ChannelBufferSize)
	pp := &partitionProducer{
		parent:    p,
		topic:     topic,
		partition: partition,
		input:     input,

		breaker:    breaker.New(3, 1, 10*time.Second),
		retryState: make([]partitionRetryState, p.conf.Producer.Retry.Max+1),
	}
	go withRecover(pp.dispatch)
	return input
}

func (pp *partitionProducer) backoff(retries int) {
	var backoff time.Duration
	if pp.parent.conf.Producer.Retry.BackoffFunc != nil {
		maxRetries := pp.parent.conf.Producer.Retry.Max
		backoff = pp.parent.conf.Producer.Retry.BackoffFunc(retries, maxRetries)
	} else {
		backoff = pp.parent.conf.Producer.Retry.Backoff
	}
	if backoff > 0 {
		time.Sleep(backoff)
	}
}

func (pp *partitionProducer) updateLeaderIfBrokerProducerIsNil(msg *ProducerMessage) error {
	if pp.brokerProducer == nil {
		if err := pp.updateLeader(); err != nil {
			pp.parent.returnError(msg, err)
			pp.backoff(msg.retries)
			return err
		}
		Logger.Printf("producer/leader/%s/%d selected broker %d\n", pp.topic, pp.partition, pp.leader.ID())
	}
	return nil
}

func (pp *partitionProducer) dispatch() {
	// try to prefetch the leader; if this doesn't work, we'll do a proper call to `updateLeader`
	// on the first message
	pp.leader, _ = pp.parent.client.Leader(pp.topic, pp.partition)
	if pp.leader != nil {
		pp.brokerProducer = pp.parent.getBrokerProducer(pp.leader)
		pp.parent.inFlight.Add(1) // we're generating a syn message; track it so we don't shut down while it's still inflight
		pp.brokerProducer.input <- &ProducerMessage{Topic: pp.topic, Partition: pp.partition, flags: syn}
	}

	defer func() {
		if pp.brokerProducer != nil {
			pp.parent.unrefBrokerProducer(pp.leader, pp.brokerProducer)
		}
	}()

	for msg := range pp.input {
		if pp.brokerProducer != nil && pp.brokerProducer.abandoned != nil {
			select {
			case <-pp.brokerProducer.abandoned:
				// a message on the abandoned channel means that our current broker selection is out of date
				Logger.Printf("producer/leader/%s/%d abandoning broker %d\n", pp.topic, pp.partition, pp.leader.ID())
				pp.parent.unrefBrokerProducer(pp.leader, pp.brokerProducer)
				pp.brokerProducer = nil
				time.Sleep(pp.parent.conf.Producer.Retry.Backoff)
			default:
				// producer connection is still open.
			}
		}

		if msg.retries > pp.highWatermark {
			if err := pp.updateLeaderIfBrokerProducerIsNil(msg); err != nil {
				continue
			}
			// a new, higher, retry level; handle it and then back off
			pp.newHighWatermark(msg.retries)
			pp.backoff(msg.retries)
		} else if pp.highWatermark > 0 {
			// we are retrying something (else highWatermark would be 0) but this message is not a *new* retry level
			if msg.retries < pp.highWatermark {
				// in fact this message is not even the current retry level, so buffer it for now (unless it's a just a fin)
				if msg.flags&fin == fin {
					pp.retryState[msg.retries].expectChaser = false
					pp.parent.inFlight.Done() // this fin is now handled and will be garbage collected
				} else {
					pp.retryState[msg.retries].buf = append(pp.retryState[msg.retries].buf, msg)
				}
				continue
			} else if msg.flags&fin == fin {
				// this message is of the current retry level (msg.retries == highWatermark) and the fin flag is set,
				// meaning this retry level is done and we can go down (at least) one level and flush that
				pp.retryState[pp.highWatermark].expectChaser = false
				pp.flushRetryBuffers()
				pp.parent.inFlight.Done() // this fin is now handled and will be garbage collected
				continue
			}
		}

		// if we made it this far then the current msg contains real data, and can be sent to the next goroutine
		// without breaking any of our ordering guarantees
		if err := pp.updateLeaderIfBrokerProducerIsNil(msg); err != nil {
			continue
		}

		// Now that we know we have a broker to actually try and send this message to, generate the sequence
		// number for it.
		// All messages being retried (sent or not) have already had their retry count updated
		// Also, ignore "special" syn/fin messages used to sync the brokerProducer and the topicProducer.
		if pp.parent.conf.Producer.Idempotent && msg.retries == 0 && msg.flags == 0 {
			if msg.hasSequence {
				panic("assertion failure: reassigning producer epoch and sequence number to message that already has them")
			}
			msg.sequenceNumber, msg.producerEpoch = pp.parent.txnmgr.getAndIncrementSequenceNumber(msg.Topic, msg.Partition)
			msg.hasSequence = true
		}

		if pp.parent.IsTransactional() {
			pp.parent.txnmgr.maybeAddPartitionToCurrentTxn(pp.topic, pp.partition)
		}

		pp.brokerProducer.input <- msg
	}
}

func (pp *partitionProducer) newHighWatermark(hwm int) {
	Logger.Printf("producer/leader/%s/%d state change to [retrying-%d]\n", pp.topic, pp.partition, hwm)
	pp.highWatermark = hwm

	// send off a fin so that we know when everything "in between" has made it
	// back to us and we can safely flush the backlog (otherwise we risk re-ordering messages)
	pp.retryState[pp.highWatermark].expectChaser = true
	pp.parent.inFlight.Add(1) // we're generating a fin message; track it so we don't shut down while it's still inflight
	pp.brokerProducer.input <- &ProducerMessage{Topic: pp.topic, Partition: pp.partition, flags: fin, retries: pp.highWatermark - 1}

	// a new HWM means that our current broker selection is out of date
	Logger.Printf("producer/leader/%s/%d abandoning broker %d\n", pp.topic, pp.partition, pp.leader.ID())
	pp.parent.unrefBrokerProducer(pp.leader, pp.brokerProducer)
	pp.brokerProducer = nil
}

func (pp *partitionProducer) flushRetryBuffers() {
	Logger.Printf("producer/leader/%s/%d state change to [flushing-%d]\n", pp.topic, pp.partition, pp.highWatermark)
	for {
		pp.highWatermark--

		if pp.brokerProducer == nil {
			if err := pp.updateLeader(); err != nil {
				pp.parent.returnErrors(pp.retryState[pp.highWatermark].buf, err)
				goto flushDone
			}
			Logger.Printf("producer/leader/%s/%d selected broker %d\n", pp.topic, pp.partition, pp.leader.ID())
		}

		for _, msg := range pp.retryState[pp.highWatermark].buf {
			if pp.parent.conf.Producer.Idempotent && msg.retries == 0 && msg.flags == 0 {
				if msg.hasSequence {
					panic("assertion failure: reassigning producer epoch and sequence number to message that already has them")
				}
				msg.sequenceNumber, msg.producerEpoch = pp.parent.txnmgr.getAndIncrementSequenceNumber(msg.Topic, msg.Partition)
				msg.hasSequence = true
			}
			pp.brokerProducer.input <- msg
		}

	flushDone:
		pp.retryState[pp.highWatermark].buf = nil
		if pp.retryState[pp.highWatermark].expectChaser {
			Logger.Printf("producer/leader/%s/%d state change to [retrying-%d]\n", pp.topic, pp.partition, pp.highWatermark)
			break
		} else if pp.highWatermark == 0 {
			Logger.Printf("producer/leader/%s/%d state change to [normal]\n", pp.topic, pp.partition)
			break
		}
	}
}

func (pp *partitionProducer) updateLeader() error {
	return pp.breaker.Run(func() (err error) {
		if err = pp.parent.client.RefreshMetadata(pp.topic); err != nil {
			return err
		}

		if pp.leader, err = pp.parent.client.Leader(pp.topic, pp.partition); err != nil {
			return err
		}

		pp.brokerProducer = pp.parent.getBrokerProducer(pp.leader)
		pp.parent.inFlight.Add(1) // we're generating a syn message; track it so we don't shut down while it's still inflight
		pp.brokerProducer.input <- &ProducerMessage{Topic: pp.topic, Partition: pp.partition, flags: syn}

		return nil
	})
}

// one per broker; also constructs an associated flusher
func (p *asyncProducer) newBrokerProducer(broker *Broker) *brokerProducer {
	var (
		input     = make(chan *ProducerMessage)
		bridge    = make(chan *produceSet)
		pending   = make(chan *brokerProducerResponse)
		responses = make(chan *brokerProducerResponse)
	)

	bp := &brokerProducer{
		parent:         p,
		broker:         broker,
		input:          input,
		output:         bridge,
		responses:      responses,
		buffer:         newProduceSet(p),
		currentRetries: make(map[string]map[int32]error),
	}
	go withRecover(bp.run)

	// minimal bridge to make the network response `select`able
	go withRecover(func() {
		// Use a wait group to know if we still have in flight requests
		var wg sync.WaitGroup

		for set := range bridge {
			request := set.buildRequest()

			// Count the in flight requests to know when we can close the pending channel safely
			wg.Add(1)

			// Capture the current set to forward in the callback
			sendResponse := func(set *produceSet) ProduceCallback {
				return func(response *ProduceResponse, err error) {
					// Forward the response to make sure we do not block the responseReceiver
					pending <- &brokerProducerResponse{
						set: set,
						err: err,
						res: response,
					}
					wg.Done()
				}
			}(set)

			if p.IsTransactional() {
				// Add partition to tx before sending current batch
				err := p.txnmgr.publishTxnPartitions()
				if err != nil {
					// Request failed to be sent
					sendResponse(nil, err)
					continue
				}
			}

			if p.conf.Producer.Idempotent || p.conf.Net.MaxOpenRequests == 1 {
				// Idempotency being enabled or MaxOpenRequests == 1 are special cases where
				// the caller has communicated: I really care about ordering. As a result, we
				// we do our best to guarantee that by using the synchronous (blocking) version
				// of produce so that there is only one outstanding Produce request at any given
				// time. A few notes:
				//
				// 1. We shouldn't *have* to do this since that is what p.conf.Net.MaxOpenRequests
				//    is for. However, as noted in the comments of the P.R that introduced pipeline
				//    in the first place, the implementation of pipelining is such that
				//    p.conf.net.MaxOpenRequests is no longer strictly honored, so we have to do it
				//    this way if we want to maintain strict ordering guarantees. P.R that introduced
				//    request pipelining: https://github.com/IBM/sarama/pull/2094
				//
				//
				// 2. In theory when idempotency is enabled, it *should* be possible to maintain
				//    strict ordering with concurrent produce requests as long as all the produce
				//    requests are received by the broker in the correct order (say, by sending them
				//    in the right order over a single connection). However, the current implementation
				//    of this library is such that idempotency breaks in certain error/retry scenarios
				//    when request pipelining is enabled. This can manifest as either the assertion in
				//    `produce_set.go` firing and complaining that a batch was created that contains out
				//    of sequence messages (https://github.com/IBM/sarama/issues/2803), or the Kafka
				//    broker itself detecting that the ordering guarantees have been violated and
				//    rejecting the batch, effectively causing data loss since there is no way for the
				//    client to retry or handle this error without risk of introducing duplicates:
				//    https://github.com/IBM/sarama/issues/2619. I wish I could say with certainty
				//    exactly *why* this happens, but after two days of strenous debugging I still can't
				//    say for sure other than this file was written with a lot of assumptiosn and things
				//    that happened to work because there was no request pipelining and the best path
				//    forward is to just make sure there is no request pipelining when strict ordering
				//    is required.
				resp, err := broker.Produce(request)
				if err != nil {
					// Request failed to be sent
					sendResponse(nil, err)
					continue
				}
				// Callback is not called when using NoResponse
				if p.conf.Producer.RequiredAcks == NoResponse {
					// Provide the expected nil response
					sendResponse(nil, nil)
				} else {
					sendResponse(resp, nil)
				}
			} else {
				// Use AsyncProduce vs Produce to not block waiting for the response
				// so that we can pipeline multiple produce requests and achieve higher throughput, see:
				// https://kafka.apache.org/protocol#protocol_network
				err := broker.AsyncProduce(request, sendResponse)
				if err != nil {
					// Request failed to be sent
					sendResponse(nil, err)
					continue
				}
				// Callback is not called when using NoResponse
				if p.conf.Producer.RequiredAcks == NoResponse {
					// Provide the expected nil response
					sendResponse(nil, nil)
				}
			}
		}
		// Wait for all in flight requests to close the pending channel safely
		wg.Wait()
		close(pending)
	})

	// In order to avoid a deadlock when closing the broker on network or malformed response error
	// we use an intermediate channel to buffer and send pending responses in order
	// This is because the AsyncProduce callback inside the bridge is invoked from the broker
	// responseReceiver goroutine and closing the broker requires such goroutine to be finished
	go withRecover(func() {
		buf := queue.New()
		for {
			if buf.Length() == 0 {
				res, ok := <-pending
				if !ok {
					// We are done forwarding the last pending response
					close(responses)
					return
				}
				buf.Add(res)
			}
			// Send the head pending response or buffer another one
			// so that we never block the callback
			headRes := buf.Peek().(*brokerProducerResponse)
			select {
			case res, ok := <-pending:
				if !ok {
					continue
				}
				buf.Add(res)
				continue
			case responses <- headRes:
				buf.Remove()
				continue
			}
		}
	})

	if p.conf.Producer.Retry.Max <= 0 {
		bp.abandoned = make(chan struct{})
	}

	return bp
}

type brokerProducerResponse struct {
	set *produceSet
	err error
	res *ProduceResponse
}

// groups messages together into appropriately-sized batches for sending to the broker
// handles state related to retries etc
type brokerProducer struct {
	parent *asyncProducer
	broker *Broker

	input     chan *ProducerMessage
	output    chan<- *produceSet
	responses <-chan *brokerProducerResponse
	abandoned chan struct{}

	buffer     *produceSet
	timer      *time.Timer
	timerFired bool

	closing        error
	currentRetries map[string]map[int32]error
}

func (bp *brokerProducer) run() {
	var output chan<- *produceSet
	var timerChan <-chan time.Time
	Logger.Printf("producer/broker/%d starting up\n", bp.broker.ID())

	for {
		select {
		case msg, ok := <-bp.input:
			if !ok {
				Logger.Printf("producer/broker/%d input chan closed\n", bp.broker.ID())
				bp.shutdown()
				return
			}

			if msg == nil {
				continue
			}

			if msg.flags&syn == syn {
				Logger.Printf("producer/broker/%d state change to [open] on %s/%d\n",
					bp.broker.ID(), msg.Topic, msg.Partition)
				if bp.currentRetries[msg.Topic] == nil {
					bp.currentRetries[msg.Topic] = make(map[int32]error)
				}
				bp.currentRetries[msg.Topic][msg.Partition] = nil
				bp.parent.inFlight.Done()
				continue
			}

			if reason := bp.needsRetry(msg); reason != nil {
				bp.parent.retryMessage(msg, reason)

				if bp.closing == nil && msg.flags&fin == fin {
					// we were retrying this partition but we can start processing again
					delete(bp.currentRetries[msg.Topic], msg.Partition)
					Logger.Printf("producer/broker/%d state change to [closed] on %s/%d\n",
						bp.broker.ID(), msg.Topic, msg.Partition)
				}

				continue
			}

			if msg.flags&fin == fin {
				// New broker producer that was caught up by the retry loop
				bp.parent.retryMessage(msg, ErrShuttingDown)
				DebugLogger.Printf("producer/broker/%d state change to [dying-%d] on %s/%d\n",
					bp.broker.ID(), msg.retries, msg.Topic, msg.Partition)
				continue
			}

			if bp.buffer.wouldOverflow(msg) {
				Logger.Printf("producer/broker/%d maximum request accumulated, waiting for space\n", bp.broker.ID())
				if err := bp.waitForSpace(msg, false); err != nil {
					bp.parent.retryMessage(msg, err)
					continue
				}
			}

			if bp.parent.txnmgr.producerID != noProducerID && bp.buffer.producerEpoch != msg.producerEpoch {
				// The epoch was reset, need to roll the buffer over
				Logger.Printf("producer/broker/%d detected epoch rollover, waiting for new buffer\n", bp.broker.ID())
				if err := bp.waitForSpace(msg, true); err != nil {
					bp.parent.retryMessage(msg, err)
					continue
				}
			}

			if bp.parent.conf.Producer.Idempotent {
				panic("msg made it to brokerProducer goroutine without being sequenced while idempotency is enabled")
			}

			if err := bp.buffer.add(msg); err != nil {
				bp.parent.returnError(msg, err)
				continue
			}

			if bp.parent.conf.Producer.Flush.Frequency > 0 && bp.timer == nil {
				bp.timer = time.NewTimer(bp.parent.conf.Producer.Flush.Frequency)
				timerChan = bp.timer.C
			}
		case <-timerChan:
			bp.timerFired = true
		case output <- bp.buffer:
			bp.rollOver()
			timerChan = nil
		case response, ok := <-bp.responses:
			if ok {
				bp.handleResponse(response)
			}
		}

		if bp.timerFired || bp.buffer.readyToFlush() {
			output = bp.output
		} else {
			output = nil
		}
	}
}

func (bp *brokerProducer) shutdown() {
	for !bp.buffer.empty() {
		select {
		case response := <-bp.responses:
			bp.handleResponse(response)
		case bp.output <- bp.buffer:
			bp.rollOver()
		}
	}
	close(bp.output)
	// Drain responses from the bridge goroutine
	for response := range bp.responses {
		bp.handleResponse(response)
	}
	// No more brokerProducer related goroutine should be running
	Logger.Printf("producer/broker/%d shut down\n", bp.broker.ID())
}

func (bp *brokerProducer) needsRetry(msg *ProducerMessage) error {
	if bp.closing != nil {
		return bp.closing
	}

	return bp.currentRetries[msg.Topic][msg.Partition]
}

func (bp *brokerProducer) waitForSpace(msg *ProducerMessage, forceRollover bool) error {
	for {
		select {
		case response := <-bp.responses:
			bp.handleResponse(response)
			// handling a response can change our state, so re-check some things
			if reason := bp.needsRetry(msg); reason != nil {
				return reason
			} else if !bp.buffer.wouldOverflow(msg) && !forceRollover {
				return nil
			}
		case bp.output <- bp.buffer:
			bp.rollOver()
			return nil
		}
	}
}

func (bp *brokerProducer) rollOver() {
	if bp.timer != nil {
		bp.timer.Stop()
	}
	bp.timer = nil
	bp.timerFired = false
	bp.buffer = newProduceSet(bp.parent)
}

func (bp *brokerProducer) handleResponse(response *brokerProducerResponse) {
	if response.err != nil {
		bp.handleError(response.set, response.err)
	} else {
		bp.handleSuccess(response.set, response.res)
	}

	if bp.buffer.empty() {
		bp.rollOver() // this can happen if the response invalidated our buffer
	}
}

func (bp *brokerProducer) handleSuccess(sent *produceSet, response *ProduceResponse) {
	// we iterate through the blocks in the request set, not the response, so that we notice
	// if the response is missing a block completely
	var retryTopics []string
	sent.eachPartition(func(topic string, partition int32, pSet *partitionSet) {
		if response == nil {
			// this only happens when RequiredAcks is NoResponse, so we have to assume success
			bp.parent.returnSuccesses(pSet.msgs)
			return
		}

		block := response.GetBlock(topic, partition)
		if block == nil {
			bp.parent.returnErrors(pSet.msgs, ErrIncompleteResponse)
			return
		}

		switch block.Err {
		// Success
		case ErrNoError:
			if bp.parent.conf.Version.IsAtLeast(V0_10_0_0) && !block.Timestamp.IsZero() {
				for _, msg := range pSet.msgs {
					msg.Timestamp = block.Timestamp
				}
			}
			for i, msg := range pSet.msgs {
				msg.Offset = block.Offset + int64(i)
			}
			bp.parent.returnSuccesses(pSet.msgs)
		// Duplicate
		case ErrDuplicateSequenceNumber:
			bp.parent.returnSuccesses(pSet.msgs)
		// Retriable errors
		case ErrInvalidMessage, ErrUnknownTopicOrPartition, ErrLeaderNotAvailable, ErrNotLeaderForPartition,
			ErrRequestTimedOut, ErrNotEnoughReplicas, ErrNotEnoughReplicasAfterAppend:
			if bp.parent.conf.Producer.Retry.Max <= 0 {
				bp.parent.abandonBrokerConnection(bp.broker)
				bp.parent.returnErrors(pSet.msgs, block.Err)
			} else {
				retryTopics = append(retryTopics, topic)
			}
		// Other non-retriable errors
		default:
			if bp.parent.conf.Producer.Retry.Max <= 0 {
				bp.parent.abandonBrokerConnection(bp.broker)
			}
			bp.parent.returnErrors(pSet.msgs, block.Err)
		}
	})

	if len(retryTopics) > 0 {
		if bp.parent.conf.Producer.Idempotent {
			err := bp.parent.client.RefreshMetadata(retryTopics...)
			if err != nil {
				Logger.Printf("Failed refreshing metadata because of %v\n", err)
			}
		}

		sent.eachPartition(func(topic string, partition int32, pSet *partitionSet) {
			block := response.GetBlock(topic, partition)
			if block == nil {
				// handled in the previous "eachPartition" loop
				return
			}

			switch block.Err {
			case ErrInvalidMessage, ErrUnknownTopicOrPartition, ErrLeaderNotAvailable, ErrNotLeaderForPartition,
				ErrRequestTimedOut, ErrNotEnoughReplicas, ErrNotEnoughReplicasAfterAppend:
				Logger.Printf("producer/broker/%d state change to [retrying] on %s/%d because %v\n",
					bp.broker.ID(), topic, partition, block.Err)
				if bp.currentRetries[topic] == nil {
					bp.currentRetries[topic] = make(map[int32]error)
				}
				bp.currentRetries[topic][partition] = block.Err
				if bp.parent.conf.Producer.Idempotent {
					// I don't really understand why we need a dedicated goroutine for this, but it was like
					// that before.
					go bp.parent.retryBatch(topic, partition, pSet, block.Err)
				} else {
					bp.parent.retryMessages(pSet.msgs, block.Err)
					// dropping the following messages has the side effect of incrementing their retry count.
					//
					// Note, I'm not 100% sure why this is here since we're not about to call bp.rollover(),
					// however, it's very important that this function is not called in the idempotency path
					// because it would break apart the messages in an existing (sequenced) batch and the
					// messages could end up split across multiple batches or in a different order which
					// violates how the idempotency protocol works and will cause an assertion to fire in the
					// client (dropping the messages) or for the Broker to reject the messages. Either way, its
					// data loss.
					bp.parent.retryMessages(bp.buffer.dropPartition(topic, partition), block.Err)
				}
			}
		})
	}
}

func (p *asyncProducer) retryBatch(topic string, partition int32, pSet *partitionSet, kerr KError) {
	// Due to how this library is structured / implemented, retryBatch can end up getting called in
	// a very tight loop in some scenarios (like when a Broker is down and the library is trying to
	// re-establish a connection). As a result, we need to make sure that retryBatch honors the
	// configured Retry.Backoff just like the regular message retries do.
	time.Sleep(p.conf.Producer.Retry.Backoff)

	Logger.Printf("Retrying batch for %v-%d because of %s\n", topic, partition, kerr)
	produceSet := newProduceSet(p)
	produceSet.msgs[topic] = make(map[int32]*partitionSet)
	produceSet.msgs[topic][partition] = pSet
	produceSet.bufferBytes += pSet.bufferBytes
	produceSet.bufferCount += len(pSet.msgs)
	// Once a producer ID and epoch have been assigned to messages, they can *never* be changed
	// in order for the idempotency protocol between the client and Broker to be correct. As a
	// result, we make sure that we copy over the old producer ID and epoch (older versions of
	// this library did not do this which resulted in bugs where the client would issue batches
	// with new producer ID / epochs, but with old sequence numbers which makes no sense).
	produceSet.producerID = pSet.recordsToSend.RecordBatch.ProducerID
	produceSet.producerEpoch = pSet.recordsToSend.RecordBatch.ProducerEpoch
	for _, msg := range pSet.msgs {
		if msg.retries >= p.conf.Producer.Retry.Max {
			p.returnErrors(pSet.msgs, kerr)
			return
		}
		msg.retries++
	}

	// Previous versions of this library did not retry the call to p.client.Leader(topic, partition)
	// if it failed. That was problematic because it meant during periods of time where we couldn't
	// figure out who the leader was (because it had just died, or there was a network problem, or
	// whatever) idempotent Produce requests would fail immediately instead of retrying for awhile
	// as expected. This retry loop is very important since prematurely (and unnecessarily) failing
	// an idempotent batch is ~equivalent to data loss.
	succeeded := false
	for i := 0; i < p.conf.Producer.Retry.Max; i++ {
		// it's expected that a metadata refresh has been requested prior to calling retryBatch
		leader, err := p.client.Leader(topic, partition)
		if err != nil {
			Logger.Printf("Failed retrying batch for %v-%d because of %v while looking up for new leader, attempt: %d of %d\n", topic, partition, err, i, p.conf.Producer.Retry.Max)
			for _, msg := range pSet.msgs {
				p.returnError(msg, kerr)
			}

			time.Sleep(p.conf.Producer.Retry.Backoff)
			continue
		}

		bp := p.getBrokerProducer(leader)
		bp.output <- produceSet
		p.unrefBrokerProducer(leader, bp)
		succeeded = true
		break
	}

	if !succeeded {
		Logger.Printf("Failed retrying batch for %v-%d because of %v while looking up for new leader, no more retries\n", topic, partition)
		for _, msg := range pSet.msgs {
			p.returnError(msg, kerr)
		}
	}
}

func (bp *brokerProducer) handleError(sent *produceSet, err error) {
	var target PacketEncodingError
	if errors.As(err, &target) {
		sent.eachPartition(func(topic string, partition int32, pSet *partitionSet) {
			bp.parent.returnErrors(pSet.msgs, err)
		})
	} else {
		Logger.Printf("producer/broker/%d state change to [closing] because %s\n", bp.broker.ID(), err)
		bp.parent.abandonBrokerConnection(bp.broker)
		_ = bp.broker.Close()
		bp.closing = err

		sent.eachPartition(func(topic string, partition int32, pSet *partitionSet) {
			// It is very important that we use retryBatch instead of retryMessages when idempotency
			// is enabled. Otherwise batches that have already been sequenced (and potentially even
			// sent to Brokers) will get split up and rebuilt in a different way, order, or with the
			// messages split across different batches. This is not allowed in the idempotency
			// protocol. The expectation on the Broker side is that if the client retries any batches,
			// it will send them with exact same messages and sequence numbers as the original attempt,
			// otherwise the protocol doesn't work.
			if bp.parent.conf.Producer.Idempotent {
				// IDK if this needs/should have an external goroutine, but the only other place where
				// retryBatch() was called did it this way, so I did the same for posterity.
				go bp.parent.retryBatch(topic, partition, pSet, ErrKafkaStorageError)
			} else {
				bp.parent.retryMessages(pSet.msgs, err)
			}
		})

		// Everything in bp.buffer needs to be retried because we're about to call bp.rollOver()
		// which will clear bp.buffer. Why we need to call bp.rollOver() here I'm not sure.
		bp.buffer.eachPartition(func(topic string, partition int32, pSet *partitionSet) {
			// Same comment as above.
			if bp.parent.conf.Producer.Idempotent {
				// Same comment as above.
				go bp.parent.retryBatch(topic, partition, pSet, ErrKafkaStorageError)
			} else {
				bp.parent.retryMessages(pSet.msgs, err)
			}
		})
		bp.rollOver()
	}
}

// singleton
// effectively a "bridge" between the flushers and the dispatcher in order to avoid deadlock
// based on https://godoc.org/github.com/eapache/channels#InfiniteChannel
func (p *asyncProducer) retryHandler() {
	var msg *ProducerMessage
	buf := queue.New()

	for {
		if buf.Length() == 0 {
			msg = <-p.retries
		} else {
			select {
			case msg = <-p.retries:
			case p.input <- buf.Peek().(*ProducerMessage):
				buf.Remove()
				continue
			}
		}

		if msg == nil {
			return
		}

		buf.Add(msg)
	}
}

// utility functions

func (p *asyncProducer) shutdown() {
	Logger.Println("Producer shutting down.")
	p.inFlight.Add(1)
	p.input <- &ProducerMessage{flags: shutdown}

	p.inFlight.Wait()

	err := p.client.Close()
	if err != nil {
		Logger.Println("producer/shutdown failed to close the embedded client:", err)
	}

	close(p.input)
	close(p.retries)
	close(p.errors)
	close(p.successes)

	p.metricsRegistry.UnregisterAll()
}

func (p *asyncProducer) bumpIdempotentProducerEpoch() {
	_, epoch := p.txnmgr.getProducerID()
	if epoch == math.MaxInt16 {
		Logger.Println("producer/txnmanager epoch exhausted, requesting new producer ID")
		txnmgr, err := newTransactionManager(p.conf, p.client)
		if err != nil {
			Logger.Println(err)
			return
		}

		p.txnmgr = txnmgr
	} else {
		p.txnmgr.bumpEpoch()
	}
}

func (p *asyncProducer) maybeTransitionToErrorState(err error) error {
	if errors.Is(err, ErrClusterAuthorizationFailed) ||
		errors.Is(err, ErrProducerFenced) ||
		errors.Is(err, ErrUnsupportedVersion) ||
		errors.Is(err, ErrTransactionalIDAuthorizationFailed) {
		return p.txnmgr.transitionTo(ProducerTxnFlagInError|ProducerTxnFlagFatalError, err)
	}
	if p.txnmgr.coordinatorSupportsBumpingEpoch && p.txnmgr.currentTxnStatus()&ProducerTxnFlagEndTransaction == 0 {
		p.txnmgr.epochBumpRequired = true
	}
	return p.txnmgr.transitionTo(ProducerTxnFlagInError|ProducerTxnFlagAbortableError, err)
}

func (p *asyncProducer) returnError(msg *ProducerMessage, err error) {
	if p.IsTransactional() {
		_ = p.maybeTransitionToErrorState(err)
	}
	// We need to reset the producer ID epoch if we set a sequence number on it, because the broker
	// will never see a message with this number, so we can never continue the sequence.
	if !p.IsTransactional() && msg.hasSequence {
		Logger.Printf("producer/txnmanager rolling over epoch due to publish failure on %s/%d", msg.Topic, msg.Partition)
		p.bumpIdempotentProducerEpoch()
	}

	msg.clear()
	pErr := &ProducerError{Msg: msg, Err: err}
	if p.conf.Producer.Return.Errors {
		p.errors <- pErr
	} else {
		Logger.Println(pErr)
	}
	p.inFlight.Done()
}

func (p *asyncProducer) returnErrors(batch []*ProducerMessage, err error) {
	for _, msg := range batch {
		p.returnError(msg, err)
	}
}

func (p *asyncProducer) returnSuccesses(batch []*ProducerMessage) {
	for _, msg := range batch {
		if p.conf.Producer.Return.Successes {
			msg.clear()
			p.successes <- msg
		}
		p.inFlight.Done()
	}
}

func (p *asyncProducer) retryMessage(msg *ProducerMessage, err error) {
	if p.conf.Producer.Idempotent && msg.hasSequence && msg.hasBeenBatched {
		// If the message has had a sequence number assigned *and* it's
		// been added to a batch (produce_set.go) then it is illegal to
		// use the retryMessage function on it because that means the
		// calling function is breaking apart and retrying messages from
		// a batch that was already potentially sent to a Kafka broker
		// which is a violation of the idempotency protocol.
		panic("retrying msg with sequence that has been batched before")
	}

	if msg.retries >= p.conf.Producer.Retry.Max {
		p.returnError(msg, err)
	} else {
		msg.retries++
		p.retries <- msg
	}
}

func (p *asyncProducer) retryMessages(batch []*ProducerMessage, err error) {
	for _, msg := range batch {
		p.retryMessage(msg, err)
	}
}

func (p *asyncProducer) getBrokerProducer(broker *Broker) *brokerProducer {
	p.brokerLock.Lock()
	defer p.brokerLock.Unlock()

	bp := p.brokers[broker]

	if bp == nil {
		bp = p.newBrokerProducer(broker)
		p.brokers[broker] = bp
		p.brokerRefs[bp] = 0
	}

	p.brokerRefs[bp]++

	return bp
}

func (p *asyncProducer) unrefBrokerProducer(broker *Broker, bp *brokerProducer) {
	p.brokerLock.Lock()
	defer p.brokerLock.Unlock()

	p.brokerRefs[bp]--
	if p.brokerRefs[bp] == 0 {
		close(bp.input)
		delete(p.brokerRefs, bp)

		if p.brokers[broker] == bp {
			delete(p.brokers, broker)
		}
	}
}

func (p *asyncProducer) abandonBrokerConnection(broker *Broker) {
	p.brokerLock.Lock()
	defer p.brokerLock.Unlock()

	bc, ok := p.brokers[broker]
	if ok && bc.abandoned != nil {
		close(bc.abandoned)
	}

	delete(p.brokers, broker)
}

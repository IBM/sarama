package sarama

import (
	"fmt"
	"sync"
	"time"
)

// OffsetMethod is passed in ConsumerConfig to tell the consumer how to determine the starting offset.
type OffsetMethod int

const (
	// OffsetMethodNewest causes the consumer to start at the most recent available offset, as
	// determined by querying the broker.
	OffsetMethodNewest OffsetMethod = iota
	// OffsetMethodOldest causes the consumer to start at the oldest available offset, as
	// determined by querying the broker.
	OffsetMethodOldest
	// OffsetMethodManual causes the consumer to interpret the OffsetValue in the ConsumerConfig as the
	// offset at which to start, allowing the user to manually specify their desired starting offset.
	OffsetMethodManual
)

// ConsumerConfig is used to pass multiple configuration options to NewConsumer.
type ConsumerConfig struct {
	// The minimum amount of data to fetch in a request - the broker will wait until at least this many bytes are available.
	// The default is 1, as 0 causes the consumer to spin when no messages are available.
	MinFetchSize int32
	// The maximum amount of time the broker will wait for MinFetchSize bytes to become available before it
	// returns fewer than that anyways. The default is 250ms, since 0 causes the consumer to spin when no events are available.
	// 100-500ms is a reasonable range for most cases. Kafka only supports precision up to milliseconds; nanoseconds will be truncated.
	MaxWaitTime time.Duration
}

// NewConsumerConfig creates a ConsumerConfig instance with sane defaults.
func NewConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		MinFetchSize: 1,
		MaxWaitTime:  250 * time.Millisecond,
	}
}

// Validate checks a ConsumerConfig instance. It will return a
// ConfigurationError if the specified value doesn't make sense.
func (config *ConsumerConfig) Validate() error {
	if config.MinFetchSize <= 0 {
		return ConfigurationError("Invalid MinFetchSize")
	}

	if config.MaxWaitTime < 1*time.Millisecond {
		return ConfigurationError("Invalid MaxWaitTime, it needs to be at least 1ms")
	} else if config.MaxWaitTime < 100*time.Millisecond {
		Logger.Println("ConsumerConfig.MaxWaitTime is very low, which can cause high CPU and network usage. See documentation for details.")
	} else if config.MaxWaitTime%time.Millisecond != 0 {
		Logger.Println("ConsumerConfig.MaxWaitTime only supports millisecond precision; nanoseconds will be truncated.")
	}

	return nil
}

// PartitionConsumerConfig is used to pass multiple configuration options to AddPartition
type PartitionConsumerConfig struct {
	// The default (maximum) amount of data to fetch from the broker in each request. The default is 32768 bytes.
	DefaultFetchSize int32
	// The maximum permittable message size - messages larger than this will return MessageTooLarge. The default of 0 is
	// treated as no limit.
	MaxMessageSize int32
	// The method used to determine at which offset to begin consuming messages. The default is to start at the most recent message.
	OffsetMethod OffsetMethod
	// Interpreted differently according to the value of OffsetMethod.
	OffsetValue int64
	// The number of events to buffer in the Events channel. Having this non-zero permits the
	// consumer to continue fetching messages in the background while client code consumes events,
	// greatly improving throughput. The default is 64.
	EventBufferSize int
}

// NewPartitionConsumerConfig creates a PartitionConsumerConfig with sane defaults.
func NewPartitionConsumerConfig() *PartitionConsumerConfig {
	return &PartitionConsumerConfig{
		DefaultFetchSize: 32768,
		EventBufferSize:  64,
	}
}

// Validate checks a PartitionConsumerConfig instance. It will return a
// ConfigurationError if the specified value doesn't make sense.
func (config *PartitionConsumerConfig) Validate() error {
	if config.DefaultFetchSize <= 0 {
		return ConfigurationError("Invalid DefaultFetchSize")
	}

	if config.MaxMessageSize < 0 {
		return ConfigurationError("Invalid MaxMessageSize")
	}

	if config.EventBufferSize < 0 {
		return ConfigurationError("Invalid EventBufferSize")
	}

	return nil
}

// ConsumerEvent is what is provided to the user when an event occurs. It is either an error (in which case Err is non-nil) or
// a message (in which case Err is nil and Offset, Key, and Value are set). Topic and Partition are always set.
type ConsumerEvent struct {
	Key, Value []byte
	Topic      string
	Partition  int32
	Offset     int64
	Err        error
}

// ConsumeErrors is a type that wraps a batch of "ConsumerEvent"s and implements the Error interface.
// It can be returned from the PartitionConsumer's Close methods to avoid the need to manually drain errors
// when stopping.
type ConsumeErrors []*ConsumerEvent

func (ce ConsumeErrors) Error() string {
	return fmt.Sprintf("kafka: %d errors when consuming", len(ce))
}

// Consumer manages PartitionConsumers which process Kafka messages from brokers.
type Consumer struct {
	client *Client
	config ConsumerConfig

	lock     sync.Mutex
	children map[string]map[int32]*PartitionConsumer
	workers  map[*Broker]*consumerWorker
}

// NewConsumer creates a new consumer attached to the given client.
func NewConsumer(client *Client, config *ConsumerConfig) (*Consumer, error) {
	// Check that we are not dealing with a closed Client before processing any other arguments
	if client.Closed() {
		return nil, ClosedClient
	}

	if config == nil {
		config = NewConsumerConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	c := &Consumer{
		client:   client,
		config:   *config,
		children: make(map[string]map[int32]*PartitionConsumer),
		workers:  make(map[*Broker]*consumerWorker),
	}

	return c, nil
}

// ConsumePartition creates a PartitionConsumer on the given topic/partition with the given configuration. It will
// return an error if this Consumer is already consuming on the given topic/partition.
func (c *Consumer) ConsumePartition(topic string, partition int32, config *PartitionConsumerConfig) (*PartitionConsumer, error) {
	if config == nil {
		config = NewPartitionConsumerConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	child := &PartitionConsumer{
		consumer:  c,
		config:    *config,
		topic:     topic,
		partition: partition,
		events:    make(chan *ConsumerEvent, config.EventBufferSize),
		trigger:   make(chan none, 1),
		dying:     make(chan none),
		fetchSize: config.DefaultFetchSize,
	}

	if err := child.chooseStartingOffset(); err != nil {
		return nil, err
	}

	if leader, err := c.client.Leader(child.topic, child.partition); err != nil {
		return nil, err
	} else {
		child.broker = leader
	}

	if err := c.addChild(child); err != nil {
		return nil, err
	}

	go withRecover(child.dispatcher)

	worker := c.refWorker(child.broker)
	worker.input <- child

	return child, nil
}

func (c *Consumer) addChild(child *PartitionConsumer) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	topicChildren := c.children[child.topic]
	if topicChildren == nil {
		topicChildren = make(map[int32]*PartitionConsumer)
		c.children[child.topic] = topicChildren
	}

	if topicChildren[child.partition] != nil {
		return ConfigurationError("That topic/partition is already being consumed")
	}

	topicChildren[child.partition] = child
	return nil
}

func (c *Consumer) removeChild(child *PartitionConsumer) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.children[child.topic], child.partition)
}

func (c *Consumer) refWorker(broker *Broker) *consumerWorker {
	c.lock.Lock()
	defer c.lock.Unlock()

	worker := c.workers[broker]
	if worker == nil {
		worker = &consumerWorker{
			consumer: c,
			broker:   broker,
			input:    make(chan *PartitionConsumer),
			newWork:  make(chan []*PartitionConsumer),
			wait:     make(chan none),
			work:     make(map[*PartitionConsumer]none),
			refs:     1,
		}
		go withRecover(worker.bridge)
		go withRecover(worker.doWork)
		c.workers[broker] = worker
	} else {
		worker.refs++
	}

	return worker
}

func (c *Consumer) unrefWorker(broker *Broker) {
	c.lock.Lock()
	defer c.lock.Unlock()

	worker := c.workers[broker]
	worker.refs--

	if worker.refs == 0 {
		close(worker.input)
		delete(c.workers, broker)
	}
}

// PartitionConsumer

// PartitionConsumer processes Kafka messages from a given topic and partition. You MUST call Close()
// on a consumer to avoid leaks, it will not be garbage-collected automatically when it passes out of
// scope (this is in addition to calling Close on the underlying consumer's client, which is still necessary).
type PartitionConsumer struct {
	consumer  *Consumer
	config    PartitionConsumerConfig
	topic     string
	partition int32

	broker         *Broker
	events         chan *ConsumerEvent
	trigger, dying chan none

	fetchSize int32
	offset    int64
}

func (child *PartitionConsumer) sendError(err error) {
	child.events <- &ConsumerEvent{
		Topic:     child.topic,
		Partition: child.partition,
		Err:       err,
	}
}

func (child *PartitionConsumer) dispatcher() {
	for _ = range child.trigger {
		select {
		case <-child.dying:
			close(child.trigger)
		default:
			if child.broker != nil {
				child.consumer.unrefWorker(child.broker)
				child.broker = nil
			}

			if err := child.dispatch(); err != nil {
				child.sendError(err)
				child.trigger <- none{}

				// there's no point in trying again *right* away
				select {
				case <-child.dying:
					close(child.trigger)
				case <-time.After(10 * time.Second):
				}
			}
		}
	}

	if child.broker != nil {
		child.consumer.unrefWorker(child.broker)
	}
	child.consumer.removeChild(child)
	close(child.events)
}

func (child *PartitionConsumer) dispatch() error {
	if err := child.consumer.client.RefreshTopicMetadata(child.topic); err != nil {
		return err
	}

	if leader, err := child.consumer.client.Leader(child.topic, child.partition); err != nil {
		return err
	} else {
		child.broker = leader
	}

	worker := child.consumer.refWorker(child.broker)

	worker.input <- child

	return nil
}

func (child *PartitionConsumer) chooseStartingOffset() (err error) {
	var where OffsetTime

	switch child.config.OffsetMethod {
	case OffsetMethodManual:
		if child.config.OffsetValue < 0 {
			return ConfigurationError("OffsetValue cannot be < 0 when OffsetMethod is MANUAL")
		}
		child.offset = child.config.OffsetValue
		return nil
	case OffsetMethodNewest:
		where = LatestOffsets
	case OffsetMethodOldest:
		where = EarliestOffset
	default:
		return ConfigurationError("Invalid OffsetMethod")
	}

	child.offset, err = child.consumer.client.GetOffset(child.topic, child.partition, where)
	return err
}

// Events returns the read channel for any events (messages or errors) that might be returned by the broker.
func (child *PartitionConsumer) Events() <-chan *ConsumerEvent {
	return child.events
}

// Close stops the PartitionConsumer from fetching messages. It is required to call this function before a
// consumer object passes out of scope, as it will otherwise leak memory. You must call this before
// calling Close on the underlying client.
func (child *PartitionConsumer) Close() error {
	// this triggers whatever worker owns this child to abandon it and close its trigger channel, which causes
	// the dispatcher to exit its loop, which removes it from the consumer then closes its 'events' channel
	// (alternatively, if the child is already at the dispatcher for some reason, that will also just
	// close itself)
	close(child.dying)

	var errors ConsumeErrors
	for event := range child.events {
		if event.Err != nil {
			errors = append(errors, event)
		}
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

// consumerWorker

type consumerWorker struct {
	consumer *Consumer
	broker   *Broker
	input    chan *PartitionConsumer
	newWork  chan []*PartitionConsumer
	wait     chan none
	work     map[*PartitionConsumer]none
	refs     int
}

func (w *consumerWorker) bridge() {
	var buffer []*PartitionConsumer

	// The bridge constantly accepts new work on `input` (even when the main worker goroutine
	// is in the middle of a network request) and batches it up. The main worker goroutine picks
	// up a batch of new work between every network request by reading from `newWork`, so we give
	// it nil if no new work is available. We also write to `wait` only when new work is available,
	// so the main goroutine can block waiting for work if it has none.
	for {
		if len(buffer) > 0 {
			select {
			case event, ok := <-w.input:
				if !ok {
					goto done
				}
				buffer = append(buffer, event)
			case w.newWork <- buffer:
				buffer = nil
			case w.wait <- none{}:
			}
		} else {
			select {
			case event, ok := <-w.input:
				if !ok {
					goto done
				}
				buffer = append(buffer, event)
			case w.newWork <- nil:
			}
		}
	}

done:
	close(w.wait)
	if len(buffer) > 0 {
		w.newWork <- buffer
	}
	close(w.newWork)
}

func (w *consumerWorker) doWork() {
	<-w.wait // wait for our first piece of work

	// the bridge ensures we will get nil right away if no new work is available
	for newWork := range w.newWork {
		w.updateWorkCache(newWork)

		if len(w.work) == 0 {
			// We're about to be shut down or we're about to receive more work.
			// Either way, the signal just hasn't propagated to our goroutine yet.
			<-w.wait
			continue
		}

		response, err := w.fetchNewMessages()

		if err != nil {
			Logger.Printf("Unexpected error processing FetchRequest; disconnecting broker %s: %s\n", w.broker.addr, err)
			w.abort(err)
			return
		}

		for child, _ := range w.work {
			block := response.GetBlock(child.topic, child.partition)
			if block == nil {
				// TODO should we be doing anything else here? The fact that we didn't get a block at all
				// (not even one with an error) suggests that the broker is misbehaving, or perhaps something in the
				// request/response pipeline is ending up malformed, so we could be better off aborting entirely...
				// on the other hand that's a sucky choice if the other partition(s) in the response have real data.
				// Hopefully this just never happens so it's a moot point :)
				child.sendError(IncompleteResponse)
				continue
			}

			w.handleResponse(child, block)
		}
	}
}

func (w *consumerWorker) updateWorkCache(newWork []*PartitionConsumer) {
	// take new work, and abandon work that has been closed
	for _, child := range newWork {
		w.work[child] = none{}
	}

	for child, _ := range w.work {
		select {
		case <-child.dying:
			close(child.trigger)
			delete(w.work, child)
		default:
		}
	}
}

func (w *consumerWorker) abort(err error) {
	_ = w.broker.Close() // we don't care about the error this might return, we already have one
	w.consumer.client.disconnectBroker(w.broker)

	for child, _ := range w.work {
		child.sendError(err)
		child.trigger <- none{}
	}

	for newWork := range w.newWork {
		for _, child := range newWork {
			child.sendError(err)
			child.trigger <- none{}
		}
	}
}

func (w *consumerWorker) fetchNewMessages() (*FetchResponse, error) {
	request := &FetchRequest{
		MinBytes:    w.consumer.config.MinFetchSize,
		MaxWaitTime: int32(w.consumer.config.MaxWaitTime / time.Millisecond),
	}

	for child, _ := range w.work {
		request.AddBlock(child.topic, child.partition, child.offset, child.fetchSize)
	}

	return w.broker.Fetch(w.consumer.client.id, request)
}

func (w *consumerWorker) handleResponse(child *PartitionConsumer, block *FetchResponseBlock) {
	switch block.Err {
	case NoError:
		break
	case UnknownTopicOrPartition, NotLeaderForPartition, LeaderNotAvailable:
		// doesn't belong to us, redispatch it
		child.trigger <- none{}
		delete(w.work, child)
		return
	default:
		child.sendError(block.Err)
		return
	}

	if len(block.MsgSet.Messages) == 0 {
		// We got no messages. If we got a trailing one then we need to ask for more data.
		// Otherwise we just poll again and wait for one to be produced...
		if block.MsgSet.PartialTrailingMessage {
			if child.config.MaxMessageSize > 0 && child.fetchSize == child.config.MaxMessageSize {
				// we can't ask for more data, we've hit the configured limit
				child.sendError(MessageTooLarge)
				child.offset++ // skip this one so we can keep processing future messages
			} else {
				child.fetchSize *= 2
				if child.config.MaxMessageSize > 0 && child.fetchSize > child.config.MaxMessageSize {
					child.fetchSize = child.config.MaxMessageSize
				}
			}
		}

		return
	}

	// we got messages, reset our fetch size in case it was increased for a previous request
	child.fetchSize = child.config.DefaultFetchSize

	atLeastOne := false
	prelude := true
	for _, msgBlock := range block.MsgSet.Messages {

		for _, msg := range msgBlock.Messages() {
			if prelude && msg.Offset < child.offset {
				continue
			}
			prelude = false

			if msg.Offset >= child.offset {
				atLeastOne = true
				child.events <- &ConsumerEvent{
					Topic:     child.topic,
					Partition: child.partition,
					Key:       msg.Msg.Key,
					Value:     msg.Msg.Value,
					Offset:    msg.Offset,
				}
				child.offset = msg.Offset + 1
			} else {
				// TODO as in doWork, should we handle this differently?
				child.sendError(IncompleteResponse)
			}
		}

	}

	if !atLeastOne {
		// TODO as in doWork, should we handle this differently?
		child.sendError(IncompleteResponse)
	}
}

package sarama

import (
	"fmt"
	"sync"
	"time"
)

// ConsumerMessage encapsulates a Kafka message returned by the consumer.
type ConsumerMessage struct {
	Key, Value []byte
	Topic      string
	Partition  int32
	Offset     int64
}

// ConsumerError is what is provided to the user when an error occurs.
// It wraps an error and includes the topic and partition.
type ConsumerError struct {
	Topic     string
	Partition int32
	Err       error
}

func (ce ConsumerError) Error() string {
	return fmt.Sprintf("kafka: error while consuming %s/%d: %s", ce.Topic, ce.Partition, ce.Err)
}

// ConsumerErrors is a type that wraps a batch of errors and implements the Error interface.
// It can be returned from the PartitionConsumer's Close methods to avoid the need to manually drain errors
// when stopping.
type ConsumerErrors []*ConsumerError

func (ce ConsumerErrors) Error() string {
	return fmt.Sprintf("kafka: %d errors while consuming", len(ce))
}

// Consumer manages PartitionConsumers which process Kafka messages from brokers. You MUST call Close()
// on a consumer to avoid leaks, it will not be garbage-collected automatically when it passes out of
// scope.
type Consumer struct {
	client    *Client
	conf      *Config
	ownClient bool

	lock            sync.Mutex
	children        map[string]map[int32]*PartitionConsumer
	brokerConsumers map[*Broker]*brokerConsumer
}

// NewConsumer creates a new consumer using the given broker addresses and configuration.
func NewConsumer(addrs []string, config *Config) (*Consumer, error) {
	client, err := NewClient(addrs, config)
	if err != nil {
		return nil, err
	}

	c, err := NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}
	c.ownClient = true
	return c, nil
}

// NewConsumerFromClient creates a new consumer using the given client.
func NewConsumerFromClient(client *Client) (*Consumer, error) {
	// Check that we are not dealing with a closed Client before processing any other arguments
	if client.Closed() {
		return nil, ErrClosedClient
	}

	c := &Consumer{
		client:          client,
		conf:            client.conf,
		children:        make(map[string]map[int32]*PartitionConsumer),
		brokerConsumers: make(map[*Broker]*brokerConsumer),
	}

	return c, nil
}

// Close shuts down the consumer. It must be called after all child PartitionConsumers have already been closed.
func (c *Consumer) Close() error {
	if c.ownClient {
		return c.client.Close()
	}
	return nil
}

const (
	// OffsetNewest causes the consumer to start at the most recent available offset, as
	// determined by querying the broker.
	OffsetNewest int64 = -1
	// OffsetOldest causes the consumer to start at the oldest available offset, as
	// determined by querying the broker.
	OffsetOldest int64 = -2
)

// ConsumePartition creates a PartitionConsumer on the given topic/partition with the given offset. It will
// return an error if this Consumer is already consuming on the given topic/partition. Offset can be a
// literal offset, or OffsetNewest or OffsetOldest
func (c *Consumer) ConsumePartition(topic string, partition int32, offset int64) (*PartitionConsumer, error) {
	child := &PartitionConsumer{
		consumer:  c,
		conf:      c.conf,
		topic:     topic,
		partition: partition,
		messages:  make(chan *ConsumerMessage, c.conf.ChannelBufferSize),
		errors:    make(chan *ConsumerError, c.conf.ChannelBufferSize),
		trigger:   make(chan none, 1),
		dying:     make(chan none),
		fetchSize: c.conf.Consumer.Fetch.Default,
	}

	if err := child.chooseStartingOffset(offset); err != nil {
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

	brokerWorker := c.refBrokerConsumer(child.broker)
	brokerWorker.input <- child

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

func (c *Consumer) refBrokerConsumer(broker *Broker) *brokerConsumer {
	c.lock.Lock()
	defer c.lock.Unlock()

	brokerWorker := c.brokerConsumers[broker]
	if brokerWorker == nil {
		brokerWorker = &brokerConsumer{
			consumer:         c,
			broker:           broker,
			input:            make(chan *PartitionConsumer),
			newSubscriptions: make(chan []*PartitionConsumer),
			wait:             make(chan none),
			subscriptions:    make(map[*PartitionConsumer]none),
			refs:             1,
		}
		go withRecover(brokerWorker.subscriptionManager)
		go withRecover(brokerWorker.subscriptionConsumer)
		c.brokerConsumers[broker] = brokerWorker
	} else {
		brokerWorker.refs++
	}

	return brokerWorker
}

func (c *Consumer) unrefBrokerConsumer(broker *Broker) {
	c.lock.Lock()
	defer c.lock.Unlock()

	brokerWorker := c.brokerConsumers[broker]
	brokerWorker.refs--

	if brokerWorker.refs == 0 {
		close(brokerWorker.input)
		delete(c.brokerConsumers, broker)
	}
}

// PartitionConsumer

// PartitionConsumer processes Kafka messages from a given topic and partition. You MUST call Close()
// on a consumer to avoid leaks, it will not be garbage-collected automatically when it passes out of
// scope (this is in addition to calling Close on the underlying consumer's client, which is still necessary).
// You have to read from both the Messages and Errors channels to prevent the consumer from locking eventually.
type PartitionConsumer struct {
	consumer  *Consumer
	conf      *Config
	topic     string
	partition int32

	broker         *Broker
	messages       chan *ConsumerMessage
	errors         chan *ConsumerError
	trigger, dying chan none

	fetchSize int32
	offset    int64
}

func (child *PartitionConsumer) sendError(err error) {
	child.errors <- &ConsumerError{
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
				child.consumer.unrefBrokerConsumer(child.broker)
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
		child.consumer.unrefBrokerConsumer(child.broker)
	}
	child.consumer.removeChild(child)
	close(child.messages)
	close(child.errors)
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

	brokerWorker := child.consumer.refBrokerConsumer(child.broker)

	brokerWorker.input <- child

	return nil
}

func (child *PartitionConsumer) chooseStartingOffset(offset int64) (err error) {
	var where OffsetTime

	switch offset {
	case OffsetNewest:
		where = LatestOffsets
	case OffsetOldest:
		where = EarliestOffset
	default:
		if offset < 0 {
			return ConfigurationError("Invalid offset")
		}
		child.offset = offset
		return nil
	}

	child.offset, err = child.consumer.client.GetOffset(child.topic, child.partition, where)
	return err
}

// Messages returns the read channel for the messages that are returned by the broker
func (child *PartitionConsumer) Messages() <-chan *ConsumerMessage {
	return child.messages
}

// Errors returns the read channel for any errors that occurred while consuming the partition.
// You have to read this channel to prevent the consumer from deadlock. Under no circumstances,
// the partition consumer will shut down by itself. It will just wait until it is able to continue
// consuming messages. If you want to shut down your consumer, you will have trigger it yourself
// by consuming this channel and calling Close or AsyncClose when appropriate.
func (child *PartitionConsumer) Errors() <-chan *ConsumerError {
	return child.errors
}

// AsyncClose initiates a shutdown of the PartitionConsumer. This method will return immediately,
// after which you should wait until the 'messages' and 'errors' channel are drained.
// It is required to call this function, or Close before a consumer object passes out of scope,
// as it will otherwise leak memory.  You must call this before calling Close on the underlying
// client.
func (child *PartitionConsumer) AsyncClose() {
	// this triggers whatever worker owns this child to abandon it and close its trigger channel, which causes
	// the dispatcher to exit its loop, which removes it from the consumer then closes its 'messages' and
	// 'errors' channel (alternatively, if the child is already at the dispatcher for some reason, that will
	// also just close itself)
	close(child.dying)
}

// Close stops the PartitionConsumer from fetching messages. It is required to call this function
// (or AsyncClose) before a consumer object passes out of scope, as it will otherwise leak memory. You must
// call this before calling Close on the underlying client.
func (child *PartitionConsumer) Close() error {
	child.AsyncClose()

	go withRecover(func() {
		for _ = range child.messages {
			// drain
		}
	})

	var errors ConsumerErrors
	for err := range child.errors {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

// brokerConsumer

type brokerConsumer struct {
	consumer         *Consumer
	broker           *Broker
	input            chan *PartitionConsumer
	newSubscriptions chan []*PartitionConsumer
	wait             chan none
	subscriptions    map[*PartitionConsumer]none
	refs             int
}

func (w *brokerConsumer) subscriptionManager() {
	var buffer []*PartitionConsumer

	// The subscriptionManager constantly accepts new subscriptions on `input` (even when the main subscriptionConsumer
	//  goroutine is in the middle of a network request) and batches it up. The main worker goroutine picks
	// up a batch of new subscriptions between every network request by reading from `newSubscriptions`, so we give
	// it nil if no new subscriptions are available. We also write to `wait` only when new subscriptions is available,
	// so the main goroutine can block waiting for work if it has none.
	for {
		if len(buffer) > 0 {
			select {
			case event, ok := <-w.input:
				if !ok {
					goto done
				}
				buffer = append(buffer, event)
			case w.newSubscriptions <- buffer:
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
			case w.newSubscriptions <- nil:
			}
		}
	}

done:
	close(w.wait)
	if len(buffer) > 0 {
		w.newSubscriptions <- buffer
	}
	close(w.newSubscriptions)
}

func (w *brokerConsumer) subscriptionConsumer() {
	<-w.wait // wait for our first piece of work

	// the subscriptionConsumer ensures we will get nil right away if no new subscriptions is available
	for newSubscriptions := range w.newSubscriptions {
		w.updateSubscriptionCache(newSubscriptions)

		if len(w.subscriptions) == 0 {
			// We're about to be shut down or we're about to receive more subscriptions.
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

		for child := range w.subscriptions {
			block := response.GetBlock(child.topic, child.partition)
			if block == nil {
				child.sendError(ErrIncompleteResponse)
				child.trigger <- none{}
				delete(w.subscriptions, child)
				continue
			}

			w.handleResponse(child, block)
		}
	}
}

func (w *brokerConsumer) updateSubscriptionCache(newSubscriptions []*PartitionConsumer) {
	// take new subscriptions, and abandon subscriptions that have been closed
	for _, child := range newSubscriptions {
		w.subscriptions[child] = none{}
	}

	for child := range w.subscriptions {
		select {
		case <-child.dying:
			close(child.trigger)
			delete(w.subscriptions, child)
		default:
		}
	}
}

func (w *brokerConsumer) abort(err error) {
	_ = w.broker.Close() // we don't care about the error this might return, we already have one
	w.consumer.client.disconnectBroker(w.broker)

	for child := range w.subscriptions {
		child.sendError(err)
		child.trigger <- none{}
	}

	for newSubscription := range w.newSubscriptions {
		for _, child := range newSubscription {
			child.sendError(err)
			child.trigger <- none{}
		}
	}
}

func (w *brokerConsumer) fetchNewMessages() (*FetchResponse, error) {
	request := &FetchRequest{
		MinBytes:    w.consumer.conf.Consumer.Fetch.Min,
		MaxWaitTime: int32(w.consumer.conf.Consumer.MaxWaitTime / time.Millisecond),
	}

	for child := range w.subscriptions {
		request.AddBlock(child.topic, child.partition, child.offset, child.fetchSize)
	}

	return w.broker.Fetch(request)
}

func (w *brokerConsumer) handleResponse(child *PartitionConsumer, block *FetchResponseBlock) {
	switch block.Err {
	case ErrNoError:
		break
	default:
		child.sendError(block.Err)
		fallthrough
	case ErrUnknownTopicOrPartition, ErrNotLeaderForPartition, ErrLeaderNotAvailable:
		// doesn't belong to us, redispatch it
		child.trigger <- none{}
		delete(w.subscriptions, child)
		return
	}

	if len(block.MsgSet.Messages) == 0 {
		// We got no messages. If we got a trailing one then we need to ask for more data.
		// Otherwise we just poll again and wait for one to be produced...
		if block.MsgSet.PartialTrailingMessage {
			if child.conf.Consumer.Fetch.Max > 0 && child.fetchSize == child.conf.Consumer.Fetch.Max {
				// we can't ask for more data, we've hit the configured limit
				child.sendError(ErrMessageTooLarge)
				child.offset++ // skip this one so we can keep processing future messages
			} else {
				child.fetchSize *= 2
				if child.conf.Consumer.Fetch.Max > 0 && child.fetchSize > child.conf.Consumer.Fetch.Max {
					child.fetchSize = child.conf.Consumer.Fetch.Max
				}
			}
		}

		return
	}

	// we got messages, reset our fetch size in case it was increased for a previous request
	child.fetchSize = child.conf.Consumer.Fetch.Default

	incomplete := false
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
				child.messages <- &ConsumerMessage{
					Topic:     child.topic,
					Partition: child.partition,
					Key:       msg.Msg.Key,
					Value:     msg.Msg.Value,
					Offset:    msg.Offset,
				}
				child.offset = msg.Offset + 1
			} else {
				incomplete = true
			}
		}

	}

	if incomplete || !atLeastOne {
		child.sendError(ErrIncompleteResponse)
		child.trigger <- none{}
		delete(w.subscriptions, child)
	}
}

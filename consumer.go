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
type Consumer interface {
	// ConsumePartition creates a PartitionConsumer on the given topic/partition with the given offset. It will
	// return an error if this Consumer is already consuming on the given topic/partition. Offset can be a
	// literal offset, or OffsetNewest or OffsetOldest
	ConsumePartition(topic string, partition int32, offset int64) (PartitionConsumer, error)

	// Close shuts down the consumer. It must be called after all child PartitionConsumers have already been closed.
	Close() error
}

type consumer struct {
	client    Client
	conf      *Config
	ownClient bool

	lock            sync.Mutex
	children        map[string]map[int32]*partitionConsumer
	brokerConsumers map[*Broker]*brokerConsumer
}

// NewConsumer creates a new consumer using the given broker addresses and configuration.
func NewConsumer(addrs []string, config *Config) (Consumer, error) {
	client, err := NewClient(addrs, config)
	if err != nil {
		return nil, err
	}

	c, err := NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}
	c.(*consumer).ownClient = true
	return c, nil
}

// NewConsumerFromClient creates a new consumer using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this consumer.
func NewConsumerFromClient(client Client) (Consumer, error) {
	// Check that we are not dealing with a closed Client before processing any other arguments
	if client.Closed() {
		return nil, ErrClosedClient
	}

	c := &consumer{
		client:          client,
		conf:            client.Config(),
		children:        make(map[string]map[int32]*partitionConsumer),
		brokerConsumers: make(map[*Broker]*brokerConsumer),
	}

	return c, nil
}

func (c *consumer) Close() error {
	if c.ownClient {
		return c.client.Close()
	}
	return nil
}

func (c *consumer) ConsumePartition(topic string, partition int32, offset int64) (PartitionConsumer, error) {
	child := &partitionConsumer{
		consumer:  c,
		conf:      c.conf,
		topic:     topic,
		partition: partition,
		messages:  make(chan *ConsumerMessage, c.conf.ChannelBufferSize),
		errors:    make(chan *ConsumerError, c.conf.ChannelBufferSize),
		responses: make(chan *FetchResponse, 1),
		results:   make(chan error, 1),
		trigger:   make(chan none, 1),
		dying:     make(chan none),
		fetchSize: c.conf.Consumer.Fetch.Default,
	}

	if err := child.chooseStartingOffset(offset); err != nil {
		return nil, err
	}

	var leader *Broker
	var err error
	if leader, err = c.client.Leader(child.topic, child.partition); err != nil {
		return nil, err
	}

	if err := c.addChild(child); err != nil {
		return nil, err
	}

	go withRecover(child.dispatcher)
	go withRecover(child.responseHandler)

	child.broker = c.refBrokerConsumer(leader)
	child.broker.input <- child

	return child, nil
}

func (c *consumer) addChild(child *partitionConsumer) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	topicChildren := c.children[child.topic]
	if topicChildren == nil {
		topicChildren = make(map[int32]*partitionConsumer)
		c.children[child.topic] = topicChildren
	}

	if topicChildren[child.partition] != nil {
		return ConfigurationError("That topic/partition is already being consumed")
	}

	topicChildren[child.partition] = child
	return nil
}

func (c *consumer) removeChild(child *partitionConsumer) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.children[child.topic], child.partition)
}

func (c *consumer) refBrokerConsumer(broker *Broker) *brokerConsumer {
	c.lock.Lock()
	defer c.lock.Unlock()

	brokerWorker := c.brokerConsumers[broker]
	if brokerWorker == nil {
		brokerWorker = &brokerConsumer{
			consumer:         c,
			broker:           broker,
			input:            make(chan *partitionConsumer),
			newSubscriptions: make(chan []*partitionConsumer),
			wait:             make(chan none),
			subscriptions:    make(map[*partitionConsumer]none),
			refs:             0,
		}
		go withRecover(brokerWorker.subscriptionManager)
		go withRecover(brokerWorker.subscriptionConsumer)
		c.brokerConsumers[broker] = brokerWorker
	}

	brokerWorker.refs++

	return brokerWorker
}

func (c *consumer) unrefBrokerConsumer(brokerWorker *brokerConsumer) {
	c.lock.Lock()
	defer c.lock.Unlock()

	brokerWorker.refs--

	if brokerWorker.refs == 0 {
		close(brokerWorker.input)
		if c.brokerConsumers[brokerWorker.broker] == brokerWorker {
			delete(c.brokerConsumers, brokerWorker.broker)
		}
	}
}

func (c *consumer) abandonBrokerConsumer(brokerWorker *brokerConsumer) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.brokerConsumers, brokerWorker.broker)
}

// PartitionConsumer

// PartitionConsumer processes Kafka messages from a given topic and partition. You MUST call Close()
// or AsyncClose() on a PartitionConsumer to avoid leaks, it will not be garbage-collected automatically
// when it passes out of scope.
//
// The simplest way of using a PartitionConsumer is to loop over its Messages channel using a for/range
// loop. The PartitionConsumer will under no circumstances stop by itself once it is started, it will
// just keep retrying if it encounters errors. By default, it logs these errors to sarama.Logger;
// if you want to handle errors yourself, set your config's Consumer.Return.Errors to true, and read
// from the Errors channel as well, using a select statement or in a separate goroutine. Check out
// the examples of Consumer to see examples of these different approaches.
type PartitionConsumer interface {

	// AsyncClose initiates a shutdown of the PartitionConsumer. This method will return immediately,
	// after which you should wait until the 'messages' and 'errors' channel are drained.
	// It is required to call this function, or Close before a consumer object passes out of scope,
	// as it will otherwise leak memory.  You must call this before calling Close on the underlying
	// client.
	AsyncClose()

	// Close stops the PartitionConsumer from fetching messages. It is required to call this function
	// (or AsyncClose) before a consumer object passes out of scope, as it will otherwise leak memory. You must
	// call this before calling Close on the underlying client.
	Close() error

	// Messages returns the read channel for the messages that are returned by the broker.
	Messages() <-chan *ConsumerMessage

	// Errors returns a read channel of errors that occured during consuming, if enabled. By default,
	// errors are logged and not returned over this channel. If you want to implement any custom errpr
	// handling, set your config's Consumer.Return.Errors setting to true, and read from this channel.
	Errors() <-chan *ConsumerError
}

type partitionConsumer struct {
	consumer  *consumer
	conf      *Config
	topic     string
	partition int32

	broker   *brokerConsumer
	messages chan *ConsumerMessage
	errors   chan *ConsumerError

	responses      chan *FetchResponse
	results        chan error
	trigger, dying chan none

	fetchSize int32
	offset    int64
}

func (child *partitionConsumer) sendError(err error) {
	cErr := &ConsumerError{
		Topic:     child.topic,
		Partition: child.partition,
		Err:       err,
	}

	if child.conf.Consumer.Return.Errors {
		child.errors <- cErr
	} else {
		Logger.Println(cErr)
	}
}

func (child *partitionConsumer) dispatcher() {
	for _ = range child.trigger {
		select {
		case <-child.dying:
			close(child.trigger)
		case <-time.After(child.conf.Consumer.Retry.Backoff):
			if child.broker != nil {
				child.consumer.unrefBrokerConsumer(child.broker)
				child.broker = nil
			}

			Logger.Printf("consumer/%s/%d finding new broker\n", child.topic, child.partition)
			if err := child.dispatch(); err != nil {
				child.sendError(err)
				child.trigger <- none{}
			}
		}
	}

	if child.broker != nil {
		child.consumer.unrefBrokerConsumer(child.broker)
	}
	child.consumer.removeChild(child)
	close(child.messages)
	close(child.errors)
	close(child.responses)
}

func (child *partitionConsumer) dispatch() error {
	if err := child.consumer.client.RefreshMetadata(child.topic); err != nil {
		return err
	}

	var leader *Broker
	var err error
	if leader, err = child.consumer.client.Leader(child.topic, child.partition); err != nil {
		return err
	}

	child.broker = child.consumer.refBrokerConsumer(leader)

	child.broker.input <- child

	return nil
}

func (child *partitionConsumer) chooseStartingOffset(offset int64) (err error) {
	var time int64

	switch offset {
	case OffsetNewest, OffsetOldest:
		time = offset
	default:
		if offset < 0 {
			return ConfigurationError("Invalid offset")
		}
		child.offset = offset
		return nil
	}

	child.offset, err = child.consumer.client.GetOffset(child.topic, child.partition, time)
	return err
}

func (child *partitionConsumer) Messages() <-chan *ConsumerMessage {
	return child.messages
}

func (child *partitionConsumer) Errors() <-chan *ConsumerError {
	return child.errors
}

func (child *partitionConsumer) AsyncClose() {
	// this triggers whatever worker owns this child to abandon it and close its trigger channel, which causes
	// the dispatcher to exit its loop, which removes it from the consumer then closes its 'messages' and
	// 'errors' channel (alternatively, if the child is already at the dispatcher for some reason, that will
	// also just close itself)
	close(child.dying)
}

func (child *partitionConsumer) Close() error {
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

func (child *partitionConsumer) responseHandler() {
	for response := range child.responses {
		ret := child.handleResponse(response)
		child.results <- ret
	}
}

func (child *partitionConsumer) handleResponse(response *FetchResponse) error {
	block := response.GetBlock(child.topic, child.partition)
	if block == nil {
		return ErrIncompleteResponse
	}

	if block.Err != ErrNoError {
		return block.Err
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

		return nil
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
		return ErrIncompleteResponse
	}
	return nil
}

// brokerConsumer

type brokerConsumer struct {
	consumer         *consumer
	broker           *Broker
	input            chan *partitionConsumer
	newSubscriptions chan []*partitionConsumer
	wait             chan none
	subscriptions    map[*partitionConsumer]none
	refs             int
}

func (w *brokerConsumer) subscriptionManager() {
	var buffer []*partitionConsumer

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
			Logger.Printf("consumer/broker/%d disconnecting due to error processing FetchRequest: %s\n", w.broker.ID(), err)
			w.abort(err)
			return
		}

		for child := range w.subscriptions {
			child.responses <- response
		}
		for child := range w.subscriptions {
			select {
			case err := <-child.results:
				if err != nil {
					switch err {
					default:
						child.sendError(err)
						fallthrough
					case ErrUnknownTopicOrPartition, ErrNotLeaderForPartition, ErrLeaderNotAvailable:
						// these three are not fatal errors, but do require redispatching
						child.trigger <- none{}
						delete(w.subscriptions, child)
						Logger.Printf("consumer/broker/%d abandoned subscription to %s/%d because %s\n", w.broker.ID(), child.topic, child.partition, err)
					}
				}
			case <-time.After(1 * time.Second):
				delete(w.subscriptions, child)
				Logger.Printf("consumer/broker/%d abandoned subscription to %s/%d because it wasn't being consumed\n", w.broker.ID(), child.topic, child.partition)
				go func(child *partitionConsumer) {
					switch err := <-child.results; err {
					case nil, ErrUnknownTopicOrPartition, ErrNotLeaderForPartition, ErrLeaderNotAvailable:
						break
					default:
						child.sendError(err)
					}
					child.trigger <- none{}
				}(child)
			}
		}
	}
}

func (w *brokerConsumer) updateSubscriptionCache(newSubscriptions []*partitionConsumer) {
	// take new subscriptions, and abandon subscriptions that have been closed
	for _, child := range newSubscriptions {
		w.subscriptions[child] = none{}
		Logger.Printf("consumer/broker/%d added subscription to %s/%d\n", w.broker.ID(), child.topic, child.partition)
	}

	for child := range w.subscriptions {
		select {
		case <-child.dying:
			close(child.trigger)
			delete(w.subscriptions, child)
			Logger.Printf("consumer/broker/%d closed dead subscription to %s/%d\n", w.broker.ID(), child.topic, child.partition)
		default:
		}
	}
}

func (w *brokerConsumer) abort(err error) {
	w.consumer.abandonBrokerConsumer(w)
	_ = w.broker.Close() // we don't care about the error this might return, we already have one

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

package sarama

import (
	"fmt"
	"sync"
	"time"
)

// ProducerConfig is used to pass multiple configuration options to NewProducer.
//
// If MaxBufferTime=MaxBufferedBytes=1, messages will be delivered immediately and
// constantly, but if multiple messages are received while a roundtrip to kafka
// is in progress, they will both be combined into the next request. In this
// mode, errors are not returned from SendMessage, but over the Errors()
// channel.
type ProducerConfig struct {
	Partitioner  Partitioner  // Chooses the partition to send messages to, or randomly if this is nil.
	RequiredAcks RequiredAcks // The level of acknowledgement reliability needed from the broker (defaults to WaitForLocal).

	// The maximum duration the broker will wait the receipt of the number of RequiredAcks.
	// This is only relevant when RequiredAcks is set to WaitForAll or a number > 1.
	// Only supports millisecond resolution, nanoseconds will be truncated.
	Timeout time.Duration

	Compression      CompressionCodec // The type of compression to use on messages (defaults to no compression).
	MaxBufferedBytes uint32           // The threshold number of bytes buffered before triggering a flush to the broker.
	MaxBufferTime    time.Duration    // The maximum duration to buffer messages before triggering a flush to the broker.

	// The maximum number of bytes allowed to accumulare in the buffer before back-pressure is applied to QueueMessage.
	// Without this, queueing messages too fast will cause the producer to construct requests larger than the MaxRequestSize.
	// Defaults to 50 MiB, cannot be more than (MaxRequestSize - 10 KiB).
	BackPressureThresholdBytes uint32
}

// Producer publishes Kafka messages. It routes messages to the correct broker
// for the provided topic-partition, refreshing metadata as appropriate, and
// parses responses for errors. You must call Close() on a producer to avoid
// leaks: it may not be garbage-collected automatically when it passes out of
// scope (this is in addition to calling Close on the underlying client, which
// is still necessary).
//
// The default values for MaxBufferedBytes and MaxBufferTime cause sarama to
// deliver messages immediately, but to buffer subsequent messages while a
// previous request is in-flight. This is often the correct behaviour.
//
// If synchronous operation is desired, you can use SendMessage. This will cause
// sarama to block until the broker has returned a value. Normally, you will
// want to use QueueMessage instead, and read the error back from the Errors()
// channel. Note that when using QueueMessage, you *must* read the values from
// the Errors() channel, or sarama will block indefinitely after a few requests.
type Producer struct {
	client          *Client
	config          ProducerConfig
	brokerProducers map[*Broker]*brokerProducer
	m               sync.RWMutex
	errors          chan error
	deliveryLocks   map[topicPartition]chan bool
	dm              sync.RWMutex
}

type brokerProducer struct {
	mapM          sync.Mutex
	messages      map[topicPartition][]*produceMessage
	bufferedBytes uint32
	flushNow      chan bool
	broker        *Broker
	stopper       chan bool
	done          chan bool
	hasMessages   chan bool
}

type topicPartition struct {
	topic     string
	partition int32
}

// NewProducer creates a new Producer using the given client.
func NewProducer(client *Client, config *ProducerConfig) (*Producer, error) {
	// Check that we are not dealing with a closed Client before processing
	// any other arguments
	if client.Closed() {
		return nil, ClosedClient
	}

	if config == nil {
		config = NewProducerConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &Producer{
		client:          client,
		config:          *config,
		errors:          make(chan error, 16),
		deliveryLocks:   make(map[topicPartition]chan bool),
		brokerProducers: make(map[*Broker]*brokerProducer),
	}, nil
}

// Errors provides access to errors generated while parsing ProduceResponses from kafka
// when operating in asynchronous mode. Should never be called in synchronous mode.
func (p *Producer) Errors() chan error {
	return p.errors
}

// Close shuts down the producer and flushes any messages it may have buffered.
// You must call this function before a producer object passes out of scope, as
// it may otherwise leak memory. You must call this before calling Close on the
// underlying client.
func (p *Producer) Close() error {
	for _, bp := range p.brokerProducers {
		bp.Close()
	}
	return nil
}

// QueueMessage sends a message with the given key and value to the given topic.
// The partition to send to is selected by the Producer's Partitioner. To send
// strings as either key or value, see the StringEncoder type.
//
// QueueMessage uses buffering semantics to reduce the nubmer of requests to the
// broker. The buffer logic is tunable with config.MaxBufferedBytes and
// config.MaxBufferTime.
//
// QueueMessage will return an error if it's unable to construct the message
// (unlikely), but network and response errors must be read from Errors(), since
// QueueMessage uses asynchronous delivery. Note that you MUST read back from
// Errors(), otherwise the producer will stall after some number of errors.
//
// If you care about message ordering, you should not call QueueMessage and
// SendMessage on the same Producer. Either, used alone, preserves ordering,
// however.
func (p *Producer) QueueMessage(topic string, key, value Encoder) error {
	return p.genericSendMessage(topic, key, value, false)
}

// SendMessage sends a message with the given key and value to the given topic.
// The partition to send to is selected by the Producer's Partitioner. To send
// strings as either key or value, see the StringEncoder type.
//
// Unlike QueueMessage, SendMessage operates synchronously, and will block until
// the response is received from the broker, returning any error generated in
// the process. Reading from Errors() may interfere with the operation of
// SendMessage().
//
// If you care about message ordering, you should not call QueueMessage and
// SendMessage on the same Producer.
func (p *Producer) SendMessage(topic string, key, value Encoder) (err error) {
	return p.genericSendMessage(topic, key, value, true)
}

func (p *Producer) genericSendMessage(topic string, key, value Encoder, synchronous bool) (err error) {
	var keyBytes, valBytes []byte

	if key != nil {
		if keyBytes, err = key.Encode(); err != nil {
			return err
		}
	}
	if value != nil {
		if valBytes, err = value.Encode(); err != nil {
			return err
		}
	}

	partition, err := p.choosePartition(topic, key)
	if err != nil {
		return err
	}

	// produce_message.go
	msg := &produceMessage{
		tp:    topicPartition{topic, partition},
		key:   keyBytes,
		value: valBytes,
		sync:  synchronous,
	}

	// produce_message.go
	return msg.enqueue(p)
}

func (p *Producer) addMessage(msg *produceMessage) error {
	bp, err := p.brokerProducerFor(msg.tp)
	if err != nil {
		return err
	}
	bp.addMessage(msg, p.config.MaxBufferedBytes, p.config.BackPressureThresholdBytes)
	return nil
}

func (p *Producer) brokerProducerFor(tp topicPartition) (*brokerProducer, error) {
	broker, err := p.client.Leader(tp.topic, tp.partition)
	if err != nil {
		return nil, err
	}

	p.m.RLock()
	bp, ok := p.brokerProducers[broker]
	p.m.RUnlock()
	if !ok {
		p.m.Lock()
		bp, ok = p.brokerProducers[broker]
		if !ok {
			bp = p.newBrokerProducer(broker)
			p.brokerProducers[broker] = bp
		}
		p.m.Unlock()
	}

	return bp, nil
}

func (p *Producer) newBrokerProducer(broker *Broker) *brokerProducer {
	bp := &brokerProducer{
		messages:    make(map[topicPartition][]*produceMessage),
		flushNow:    make(chan bool, 1),
		broker:      broker,
		stopper:     make(chan bool),
		done:        make(chan bool),
		hasMessages: make(chan bool, 1),
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		timer := time.NewTimer(p.config.MaxBufferTime)
		var shutdownRequired bool
		wg.Done()
		for {
			select {
			case <-bp.flushNow:
				if shutdownRequired = bp.flush(p); shutdownRequired {
					goto shutdown
				}
			case <-timer.C:
				if shutdownRequired = bp.flushIfAnyMessages(p); shutdownRequired {
					goto shutdown
				}
			case <-bp.stopper:
				goto shutdown
			}
			timer.Reset(p.config.MaxBufferTime)
		}
	shutdown:
		delete(p.brokerProducers, bp.broker)
		bp.flushIfAnyMessages(p)
		if shutdownRequired {
			p.client.disconnectBroker(bp.broker)
		}
		close(bp.flushNow)
		close(bp.hasMessages)
		close(bp.done)
	}()
	wg.Wait() // don't return until the G has started

	return bp
}

func (bp *brokerProducer) addMessage(msg *produceMessage, maxBufferBytes, backPressureThreshold uint32) {
	bp.mapM.Lock()
	if msg.retried {
		// Prepend: Deliver first, before any more recently-added messages.
		bp.messages[msg.tp] = append([]*produceMessage{msg}, bp.messages[msg.tp]...)
	} else {
		// Append
		bp.messages[msg.tp] = append(bp.messages[msg.tp], msg)
	}
	bp.bufferedBytes += msg.byteSize()

	select {
	case bp.hasMessages <- true:
	default:
	}

	bp.mapM.Unlock()
	bp.flushIfOverCapacity(maxBufferBytes, backPressureThreshold)
}

func (bp *brokerProducer) flushIfOverCapacity(maxBufferBytes, backPressureThreshold uint32) {
	bp.mapM.Lock()
	softlimit := bp.bufferedBytes > maxBufferBytes
	hardlimit := bp.bufferedBytes > backPressureThreshold
	bp.mapM.Unlock()

	if hardlimit {
		bp.flushNow <- true
	} else if softlimit {
		select {
		case bp.flushNow <- true:
		default:
		}
	}
}

func (bp *brokerProducer) flushIfAnyMessages(p *Producer) (shutdownRequired bool) {
	select {
	case <-bp.hasMessages:
		select {
		case bp.hasMessages <- true:
		default:
		}
		return bp.flush(p)
	default:
	}
	return false
}

func (bp *brokerProducer) flush(p *Producer) (shutdownRequired bool) {
	var prb produceRequestBuilder

	// only deliver messages for topic-partitions that are not currently being delivered.
	bp.mapM.Lock()
	for tp, messages := range bp.messages {
		if len(messages) > 0 && p.tryAcquireDeliveryLock(tp) {
			prb = append(prb, messages...)
			delete(bp.messages, tp)
			p.releaseDeliveryLock(tp)
		}
	}
	bp.mapM.Unlock()

	if len(prb) > 0 {
		bp.mapM.Lock()
		bp.bufferedBytes -= prb.byteSize()
		bp.mapM.Unlock()

		return bp.flushRequest(p, prb, func(err error) {
			if err != nil {
				Logger.Println(err)
			}
			p.errors <- err
		})
	}
	return false
}

func (bp *brokerProducer) flushRequest(p *Producer, prb produceRequestBuilder, errorCb func(error)) (shutdownRequired bool) {
	// produce_message.go
	req := prb.toRequest(&p.config)
	response, err := bp.broker.Produce(p.client.id, req)

	switch err {
	case nil:
		break
	case EncodingError:
		// No sense in retrying; it'll just fail again. But what about all the other
		// messages that weren't invalid? Really, this is a "shit's broke real good"
		// scenario, so logging it and moving on is probably acceptable.
		errorCb(err)
		return false
	default:
		p.client.disconnectBroker(bp.broker)
		overlimit := 0
		prb.reverseEach(func(msg *produceMessage) {
			if err := msg.reenqueue(p); err != nil {
				overlimit++
			}
		})
		if overlimit > 0 {
			errorCb(DroppedMessagesError{overlimit, err})
		} else {
			errorCb(nil)
		}
		return true
	}

	// When does this ever actually happen, and why don't we explode when it does?
	// This seems bad.
	if response == nil {
		errorCb(nil)
		return false
	}

	seenPartitions := false
	for topic, d := range response.Blocks {
		for partition, block := range d {
			seenPartitions = true

			if block == nil {
				// IncompleteResponse. Here we just drop all the messages; we don't know whether
				// they were successfully sent or not. Non-ideal, but how often does it happen?
				errorCb(DroppedMessagesError{len(prb), IncompleteResponse})
			}
			switch block.Err {
			case NoError:
				// All the messages for this topic-partition were delivered successfully!
				// Unlock delivery for this topic-partition and discard the produceMessage objects.
				errorCb(nil)
			case UnknownTopicOrPartition, NotLeaderForPartition, LeaderNotAvailable:
				p.client.RefreshTopicMetadata(topic)

				overlimit := 0
				prb.reverseEach(func(msg *produceMessage) {
					if msg.hasTopicPartition(topic, partition) {
						if err := msg.reenqueue(p); err != nil {
							overlimit++
						}
					}
				})
				if overlimit > 0 {
					errorCb(DroppedMessagesError{overlimit, err})
				} else {
					errorCb(nil)
				}
			default:
				errorCb(DroppedMessagesError{len(prb), err})
			}
		}
	}

	if !seenPartitions {
		errorCb(DroppedMessagesError{len(prb), IncompleteResponse})
	}

	return false
}

func (bp *brokerProducer) Close() error {
	select {
	case <-bp.stopper:
		return fmt.Errorf("already closed or closing")
	default:
		close(bp.stopper)
		<-bp.done
	}
	return nil
}

func (p *Producer) tryAcquireDeliveryLock(tp topicPartition) bool {
	p.dm.RLock()
	ch, ok := p.deliveryLocks[tp]
	p.dm.RUnlock()
	if !ok {
		p.dm.Lock()
		ch, ok = p.deliveryLocks[tp]
		if !ok {
			ch = make(chan bool, 1)
			p.deliveryLocks[tp] = ch
		}
		p.dm.Unlock()
	}

	select {
	case ch <- true:
		return true
	default:
		return false
	}
}

func (p *Producer) releaseDeliveryLock(tp topicPartition) {
	p.dm.RLock()
	ch := p.deliveryLocks[tp]
	p.dm.RUnlock()
	select {
	case <-ch:
	default:
		panic("Serious logic bug: releaseDeliveryLock called without acquiring lock first.")
	}
}

func (p *Producer) choosePartition(topic string, key Encoder) (int32, error) {
	partitions, err := p.client.Partitions(topic)
	if err != nil {
		return -1, err
	}

	numPartitions := int32(len(partitions))

	if numPartitions == 0 {
		return -1, LeaderNotAvailable
	}

	choice := p.config.Partitioner.Partition(key, numPartitions)

	if choice < 0 || choice >= numPartitions {
		return -1, InvalidPartition
	}

	return partitions[choice], nil
}

// NewProducerConfig creates a new ProducerConfig instance with sensible defaults.
func NewProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		Partitioner:                NewRandomPartitioner(),
		RequiredAcks:               WaitForLocal,
		MaxBufferTime:              1 * time.Millisecond,
		MaxBufferedBytes:           1,
		BackPressureThresholdBytes: 50 * 1024 * 1024,
	}
}

// Validate checks a ProducerConfig instance. It will return a
// ConfigurationError if the specified value doesn't make sense.
func (config *ProducerConfig) Validate() error {
	if config.RequiredAcks < -1 {
		return ConfigurationError("Invalid RequiredAcks")
	}

	if config.Timeout < 0 {
		return ConfigurationError("Invalid Timeout")
	} else if config.Timeout%time.Millisecond != 0 {
		Logger.Println("ProducerConfig.Timeout only supports millisecond resolution; nanoseconds will be truncated.")
	}

	if config.MaxBufferedBytes == 0 {
		return ConfigurationError("Invalid MaxBufferedBytes")
	}

	if config.MaxBufferTime == 0 {
		return ConfigurationError("Invalid MaxBufferTime")
	}

	if config.Partitioner == nil {
		return ConfigurationError("No partitioner set")
	}

	if config.BackPressureThresholdBytes < config.MaxBufferedBytes {
		return ConfigurationError("BackPressureThresholdBytes cannot be less than MaxBufferedBytes")
	}

	if config.BackPressureThresholdBytes > MaxRequestSize-10*1024 {
		return ConfigurationError("BackPressureThresholdBytes must be at least 10KiB less than MaxRequestSize")
	}

	return nil
}

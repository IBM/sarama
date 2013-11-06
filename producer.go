package sarama

import (
	"fmt"
	"sync"
	"time"
)

// ProducerConfig is used to pass multiple configuration options to NewProducer.
//
// If MaxBufferTime=MaxBufferBytes=0, messages will be delivered immediately and
// constantly, but if multiple messages are received while a roundtrip to kafka
// is in progress, they will both be combined into the next request. In this
// mode, errors are not returned from SendMessage, but over the Errors()
// channel.
//
// With MaxBufferTime and/or MaxBufferBytes set to values > 0, sarama will
// buffer messages before sending, to reduce traffic.
type ProducerConfig struct {
	Partitioner        Partitioner      // Chooses the partition to send messages to, or randomly if this is nil.
	RequiredAcks       RequiredAcks     // The level of acknowledgement reliability needed from the broker (defaults to no acknowledgement).
	Timeout            int32            // The maximum time in ms the broker will wait the receipt of the number of RequiredAcks.
	Compression        CompressionCodec // The type of compression to use on messages (defaults to no compression).
	MaxBufferBytes     uint32           // The maximum number of bytes to buffer per-broker before sending to Kafka.
	MaxBufferTime      uint32           // The maximum number of milliseconds to buffer messages before sending to a broker.
	MaxDeliveryRetries uint32           // The number of times to retry a failed message. You should always specify at least 1.
}

// Producer publishes Kafka messages. It routes messages to the correct broker
// for the provided topic-partition, refreshing metadata as appropriate, and
// parses responses for errors. You must call Close() on a producer to avoid
// leaks: it may not be garbage-collected automatically when it passes out of
// scope (this is in addition to calling Close on the underlying client, which
// is still necessary).
//
// If MaxBufferBytes=0 and MaxBufferTime=0, the Producer is considered to be
// operating in "synchronous" mode. This means that errors will be returned
// directly from calls to SendMessage. If either value is greater than zero, the
// Producer is operating in "asynchronous" mode, and you must read these return
// values back from the channel returned by Errors(). Note that you actually
// *must* read these error values: The channel has a fixed capacity, and the
// producer will block if it's full.
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
	if config == nil {
		config = new(ProducerConfig)
	}

	if config.RequiredAcks < -1 {
		return nil, ConfigurationError("Invalid RequiredAcks")
	}

	if config.Timeout < 0 {
		return nil, ConfigurationError("Invalid Timeout")
	}

	if config.MaxDeliveryRetries < 1 {
		Logger.Println("Warning: config.MaxDeliveryRetries is set dangerously low. This will lead to occasional data loss.")
	}

	if config.Partitioner == nil {
		config.Partitioner = NewRandomPartitioner()
	}

	if config.MaxBufferBytes == 0 {
		config.MaxBufferBytes = 1
	}

	return &Producer{
		client:          client,
		config:          *config,
		errors:          make(chan error, 16),
		deliveryLocks:   make(map[topicPartition]chan bool),
		brokerProducers: make(map[*Broker]*brokerProducer),
	}, nil
}

// When operating in asynchronous mode, provides access to errors generated
// while parsing ProduceResponses from kafka. Should never be called in
// synchronous mode.
func (p *Producer) Errors() chan error {
	return p.errors
}

// Close shuts down the producer and flushes any messages it may have buffered.
// You must call this function before a producer object passes out of scope, as
// it may otherwise leak memory. You must call this before calling Close on the
// underlying client.
func (p *Producer) Close() error {
	p.m.Lock()
	defer p.m.Unlock()
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
// broker. The buffer logic is tunable with config.MaxBufferBytes and
// config.MaxBufferTime.
//
// QueueMessage will return an error if it's unable to construct the message
// (unlikely), but network and response errors must be read from Errors(), since
// QueueMessage uses asynchronous delivery. Note that you MUST read back from
// Errors(), otherwise the producer will stall after some number of errors.
//
// If you care about message ordering, you should not call QueueMessage and
// SendMessage on the same Producer.
func (p *Producer) QueueMessage(topic string, key, value Encoder) (err error) {
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

	msg := &produceMessage{
		tp:       topicPartition{topic, partition},
		key:      keyBytes,
		value:    valBytes,
		failures: 0,
	}

	return p.addMessage(msg)
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
func (p *Producer) SendMessage(topic string, key, value Encoder) error {
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

	msg := &produceMessage{
		tp:       topicPartition{topic, partition},
		key:      keyBytes,
		value:    valBytes,
		failures: 0,
	}

}

func (p *Producer) addMessage(msg *produceMessage) error {
	bp, err := p.brokerProducerFor(msg.tp)
	if err != nil {
		return err
	}
	bp.addMessage(msg, p.config.MaxBufferBytes)
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

	maxBufferTime := time.Duration(p.config.MaxBufferTime) * time.Millisecond

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		timer := time.NewTimer(maxBufferTime)
		wg.Done()
		for {
			println("SEL")
			select {
			case <-bp.flushNow:
				bp.flush(p)
			case <-timer.C:
				bp.flushIfAnyMessages(p)
			case <-bp.stopper:
				delete(p.brokerProducers, bp.broker)
				bp.flushIfAnyMessages(p)
				p.client.disconnectBroker(bp.broker)
				close(bp.flushNow)
				close(bp.hasMessages)
				close(bp.done)
				return
			}
			timer.Reset(maxBufferTime)
		}
	}()
	wg.Wait() // don't return until the G has started

	return bp
}

func (bp *brokerProducer) addMessage(msg *produceMessage, maxBufferBytes uint32) {
	bp.mapM.Lock()
	if msg.failures > 0 {
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
	bp.flushIfOverCapacity(maxBufferBytes)
}

func (bp *brokerProducer) flushIfOverCapacity(maxBufferBytes uint32) {
	if bp.bufferedBytes > maxBufferBytes {
		select {
		case bp.flushNow <- true:
		default:
		}
	}
}

func (bp *brokerProducer) flushIfAnyMessages(p *Producer) {
	select {
	case <-bp.hasMessages:
		bp.hasMessages <- true
		bp.flush(p)
	default:
	}
}

func (bp *brokerProducer) flush(p *Producer) {
	var prb produceRequestBuilder
	fmt.Println("FLUSHING")

	// only deliver messages for topic-partitions that are not currently being delivered.
	bp.mapM.Lock()
	for tp, messages := range bp.messages {
		if len(messages) > 0 && p.tryAcquireDeliveryLock(tp) {
			defer p.releaseDeliveryLock(tp)
			prb = append(prb, messages...)
			delete(bp.messages, tp)
		}
	}
	bp.mapM.Unlock()

	if len(prb) > 0 {
		bp.mapM.Lock()
		bp.bufferedBytes -= prb.byteSize()
		bp.mapM.Unlock()

		bp.flushRequest(p, prb, func(err error) {
			p.errors <- err
		})
	}
}

func (bp *brokerProducer) CloseAndDisconnect(client *Client) {
	broker := bp.broker
	bp.Close()
	client.disconnectBroker(broker)
}

func (bp *brokerProducer) flushRequest(p *Producer, prb produceRequestBuilder, errorCb func(error)) {
	req := prb.toRequest(&p.config)
	response, err := bp.broker.Produce(p.client.id, req)

	switch err {
	case nil:
		break
	case EncodingError:
		// No sense in retrying; it'll just fail again. But what about all the other
		// messages that weren't invalid? Really, this is a "shit's broke real good"
		// scenario, so logging it and moving on is probably acceptable.
		Logger.Printf("[DATA LOSS] EncodingError! Dropped %d messages.\n", len(prb))
		errorCb(err)
		return
	default:
		bp.CloseAndDisconnect(p.client)

		overlimit := 0
		prb.reverseEach(func(msg *produceMessage) {
			if ok := msg.reenqueue(p); !ok {
				overlimit++
			}
		})
		if overlimit > 0 {
			Logger.Printf("[DATA LOSS] %d messages exceeded the retry limit of %d and were dropped.\n",
				overlimit, p.config.MaxDeliveryRetries)
			// TODO errorCb() ???
		}
		return
	}

	// When does this ever actually happen, and why don't we explode when it does?
	// This seems bad.
	if response == nil {
		errorCb(nil)
		return
	}

	for topic, d := range response.Blocks {
		for partition, block := range d {
			if block == nil {
				// IncompleteResponse. Here we just drop all the messages; we don't know whether
				// they were successfully sent or not. Non-ideal, but how often does it happen?
				Logger.Printf("[DATA LOSS] IncompleteResponse: up to %d messages for %s:%d are in an unknown state\n",
					len(prb), topic, partition)
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
						if ok := msg.reenqueue(p); !ok {
							overlimit++
						}
					}
				})
				if overlimit > 0 {
					Logger.Printf("[DATA LOSS] %d messages exceeded the retry limit of %d and were dropped.\n",
						overlimit, p.config.MaxDeliveryRetries)
				}
			default:
				Logger.Printf("[DATA LOSS] Non-retriable error from kafka! Dropped up to %d messages for %s:%d.\n",
					len(prb), topic, partition)
			}
		}
	}
}

func (bp *brokerProducer) Close() error {
	close(bp.stopper)
	<-bp.done
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

	choice := p.config.Partitioner.Partition(key, numPartitions)

	if choice < 0 || choice >= numPartitions {
		return -1, InvalidPartition
	}

	return partitions[choice], nil
}

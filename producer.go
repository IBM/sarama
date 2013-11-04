package sarama

import (
	"sync"
	"time"
)

// ProducerConfig is used to pass multiple configuration options to NewProducer.
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
// values back from the channel returned by Errors().
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
	messages      map[string]map[int32][]*produceMessage
	bufferedBytes uint32
	flushNow      chan bool
	broker        *Broker
	stopper       chan bool
	hasMessages   chan bool
}

type produceMessage struct {
	topic      string
	partition  int32
	key, value []byte
	failures   uint32
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
	if p.isSynchronous() {
		panic("use of Errors() is not permitted in synchronous mode.")
	} else {
		return p.errors
	}
}

// Close shuts down the producer and flushes any messages it may have buffered.
// You must call this function before a producer object passes out of scope, as
// it may otherwise leak memory. You must call this before calling Close on the
// underlying client.
func (p *Producer) Close() error {
	// TODO
	return nil
}

// SendMessage sends a message with the given key and value to the given topic.
// The partition to send to is selected by the Producer's Partitioner. To send
// strings as either key or value, see the StringEncoder type.
//
// If operating in synchronous mode, a nil return indicates everything happened
// successfully. In asynchronous mode, a nil return only means that the data was
// successfully sent to kafka, and you must listen to the channel returned by
// Errors() for any errors generated later when the response is received.
func (p *Producer) SendMessage(topic string, key, value Encoder) (err error) {
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
		topic:     topic,
		partition: partition,
		key:       keyBytes,
		value:     valBytes,
		failures:  0,
	}

	return p.addMessage(msg, false)
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

func (p *Producer) addMessage(msg *produceMessage, isRetry bool) error {
	broker, err := p.client.Leader(msg.topic, msg.partition)
	if err != nil {
		return err
	}

	bp := p.brokerProducerFor(broker)
	bp.addMessage(msg, p.config.MaxBufferBytes, isRetry)

	if p.isSynchronous() {
		return <-p.errors
	}
	return nil
}

func (p *Producer) isSynchronous() bool {
	return p.config.MaxBufferBytes < 2 && p.config.MaxBufferTime == 0
}

func (p *Producer) brokerProducerFor(broker *Broker) *brokerProducer {
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
	return bp
}

func (p *Producer) newBrokerProducer(broker *Broker) *brokerProducer {
	bp := &brokerProducer{
		messages:    make(map[string]map[int32][]*produceMessage),
		flushNow:    make(chan bool, 1),
		broker:      broker,
		stopper:     make(chan bool),
		hasMessages: make(chan bool, 1),
	}

	maxBufferTime := time.Duration(p.config.MaxBufferTime) * time.Millisecond

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		timer := time.NewTimer(maxBufferTime)
		wg.Done()
		for {
			select {
			case <-bp.flushNow:
				bp.flush(p)
			case <-timer.C:
				bp.flush(p)
			case <-bp.stopper:
				p.m.Lock()
				delete(p.brokerProducers, bp.broker)
				p.m.Unlock()
				bp.flush(p)
				p.client.disconnectBroker(bp.broker)
				close(bp.flushNow)
				close(bp.hasMessages)
				return
			}
			timer.Reset(maxBufferTime)
		}
	}()
	wg.Wait()

	return bp
}

func (bp *brokerProducer) addMessage(msg *produceMessage, maxBufferBytes uint32, isRetry bool) {
	bp.mapM.Lock()
	forTopic, ok := bp.messages[msg.topic]
	if !ok {
		forTopic = make(map[int32][]*produceMessage)
		bp.messages[msg.topic] = forTopic
	}
	if isRetry {
		// Prepend: Deliver first.
		forTopic[msg.partition] = append([]*produceMessage{msg}, forTopic[msg.partition]...)
	} else {
		// Append
		forTopic[msg.partition] = append(forTopic[msg.partition], msg)
	}
	bp.bufferedBytes += uint32(len(msg.key) + len(msg.value))

	select {
	case bp.hasMessages <- true:
	default:
	}

	bp.mapM.Unlock()
	if bp.bufferedBytes > maxBufferBytes {
		bp.tryFlush()
	}
}

func (bp *brokerProducer) tryFlush() {
	select {
	case bp.flushNow <- true:
	default:
	}
}

func (bp *brokerProducer) flush(p *Producer) {
	// try to acquire delivery locks for each topic-partition involved.

	var messagesToSend []*produceMessage

	<-bp.hasMessages // wait for a message if the BP currently has none.

	bp.mapM.Lock()
	for topic, m := range bp.messages {
		for partition, messages := range m {
			if p.tryAcquireDeliveryLock(topic, partition) {

				messagesToSend = append(messagesToSend, messages...)
				m[partition] = nil

			}
		}
	}
	bp.mapM.Unlock()

	go bp.flushMessages(p, messagesToSend)
}

func (bp *brokerProducer) flushMessages(p *Producer, messages []*produceMessage) {
	if len(messages) == 0 {
		return
	}

	req := &ProduceRequest{RequiredAcks: p.config.RequiredAcks, Timeout: p.config.Timeout}
	for _, pmsg := range messages {
		msg := &Message{Codec: p.config.Compression, Key: pmsg.key, Value: pmsg.value}
		req.AddMessage(pmsg.topic, pmsg.partition, msg)
	}

	bp.flushRequest(p, req, messages)
}

func (bp *brokerProducer) Close() error {
	close(bp.stopper)
	return nil
}

func (bp *brokerProducer) flushRequest(p *Producer, request *ProduceRequest, messages []*produceMessage) {
	response, err := bp.broker.Produce(p.client.id, request)

	size := 0
	for _, m := range messages {
		size += len(m.key) + len(m.value)
	}
	bp.mapM.Lock()
	bp.bufferedBytes -= uint32(size)
	bp.mapM.Unlock()

	switch err {
	case nil:
		break
	case EncodingError:
		// No sense in retrying; it'll just fail again. But what about all the other
		// messages that weren't invalid? Really, this is a "shit's broke real good"
		// scenario, so logging it and moving on is probably acceptable.
		Logger.Printf("[DATA LOSS] EncodingError! Dropped %d messages.\n", len(messages))
		p.errors <- err
		goto releaseAllLocks
	default:
		p.client.disconnectBroker(bp.broker)
		bp.Close()

		overlimit := 0
		// ie. for msg := range reverse(messages)
		for i := len(messages) - 1; i >= 0; i-- {
			msg := messages[i]
			if msg.failures < p.config.MaxDeliveryRetries {
				msg.failures++
				// Passing isRetry=true causes the message to happen before other queued messages.
				// This is also why we have to iterate backwards through the failed messages --
				// to preserve ordering, we have to prepend the items starting from the last one.
				p.addMessage(msg, true)
			} else {
				overlimit++
				// log about message failing too many times?
			}
		}
		if overlimit > 0 {
			Logger.Printf("[DATA LOSS] %d messages exceeded the retry limit of %d and were dropped.\n",
				overlimit, p.config.MaxDeliveryRetries)
		}
		goto releaseAllLocks
	}

	// When does this ever actually happen, and why don't we explode when it does?
	// This seems bad.
	if response == nil {
		p.errors <- nil
		goto releaseAllLocks
	}

	for topic, d := range response.Blocks {
		for partition, block := range d {
			if block == nil {
				// IncompleteResponse. Here we just drop all the messages; we don't know whether
				// they were successfully sent or not. Non-ideal, but how often does it happen?
				Logger.Printf("[DATA LOSS] IncompleteResponse: up to %d messages for %s:%d are in an unknown state\n",
					len(messages), topic, partition)
			}
			switch block.Err {
			case NoError:
				// All the messages for this topic-partition were delivered successfully!
				// Unlock delivery for this topic-partition and discard the produceMessage objects.
				p.errors <- nil
			case UnknownTopicOrPartition, NotLeaderForPartition, LeaderNotAvailable:
				p.client.RefreshTopicMetadata(topic)

				overlimit := 0
				// ie. for msg := range reverse(messages)
				for i := len(messages) - 1; i >= 0; i-- {
					msg := messages[i]
					if msg.topic == topic && msg.partition == partition {
						if msg.failures < p.config.MaxDeliveryRetries {
							msg.failures++
							// Passing isRetry=true causes the message to happen before other queued messages.
							// This is also why we have to iterate backwards through the failed messages --
							// to preserve ordering, we have to prepend the items starting from the last one.
							p.addMessage(msg, true)
						} else {
							overlimit++
						}
					}
				}
				if overlimit > 0 {
					Logger.Printf("[DATA LOSS] %d messages exceeded the retry limit of %d and were dropped.\n",
						overlimit, p.config.MaxDeliveryRetries)
				}
			default:
				Logger.Printf("[DATA LOSS] Non-retriable error from kafka! Dropped up to %d messages for %s:%d.\n",
					len(messages), topic, partition)
			}
			p.releaseDeliveryLock(topic, partition)
		}
	}

	return

releaseAllLocks:
	// This is slow, but only happens on rare error conditions.

	tps := make(map[string]map[int32]bool)
	for _, msg := range messages {
		forTopic, ok := tps[msg.topic]
		if !ok {
			forTopic = make(map[int32]bool)
			tps[msg.topic] = forTopic
		}
		forTopic[msg.partition] = true
	}

	for topic, d := range tps {
		for partition := range d {
			p.releaseDeliveryLock(topic, partition)
		}
	}
}

func (p *Producer) tryAcquireDeliveryLock(topic string, partition int32) bool {
	tp := topicPartition{topic, partition}
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

func (p *Producer) releaseDeliveryLock(topic string, partition int32) {
	p.dm.RLock()
	ch := p.deliveryLocks[topicPartition{topic, partition}]
	p.dm.RUnlock()
	select {
	case <-ch:
	default:
		panic("Serious logic bug: releaseDeliveryLock called without acquiring lock first.")
	}
}

package sarama

import (
	"errors"
	"time"
)

// ProducerConfig is used to pass multiple configuration options to NewProducer.
type ProducerConfig struct {
	Partitioner         Partitioner      // Chooses the partition to send messages to, or randomly if this is nil.
	RequiredAcks        RequiredAcks     // The level of acknowledgement reliability needed from the broker (defaults to no acknowledgement).
	Timeout             int32            // The maximum time in ms the broker will wait the receipt of the number of RequiredAcks.
	Compression         CompressionCodec // The type of compression to use on messages (defaults to no compression).
	MaxBufferedMessages uint             // The maximum number of messages permitted to buffer before flushing.
	MaxBufferedBytes    uint             // The maximum number of message bytes permitted to buffer before flushing.
	MaxBufferTime       time.Duration    // The maximum amount of time permitted to buffer before flushing (or zero for no timer).
	AckSuccesses        bool             // When true, every successful delivery causes a nil to be sent on the Errors channel.
}

// Producer publishes Kafka messages. It routes messages to the correct broker, refreshing metadata as appropriate,
// and parses responses for errors. You must call Close() on a producer to avoid leaks, it may not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
type Producer struct {
	client     *Client
	dispatcher *dispatcher
	config     ProducerConfig
	errors     chan *ProduceError
	input      chan *MessageToSend
	stopper    chan bool
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

	if config.Partitioner == nil {
		config.Partitioner = NewRandomPartitioner()
	}

	prod := &Producer{client, nil, *config,
		make(chan *ProduceError, 32),
		make(chan *MessageToSend, 32),
		make(chan bool),
	}
	prod.dispatcher = &dispatcher{
		make(chan *pendingMessage, 32),
		prod,
		make(map[string]map[int32]*msgQueue),
		make(map[*Broker]*batcher),
	}

	go prod.receive()
	go prod.dispatcher.dispatch()

	return prod, nil
}

// Close shuts down the producer and flushes any messages it may have buffered. You must call this function before
// a producer object passes out of scope, as it may otherwise leak memory. You must call this before calling Close
// on the underlying client.
func (p *Producer) Close() error {
	close(p.input)
	<-p.stopper
	p.dispatcher.msgs <- nil
	return nil
}

// MessageToSend is the collection of elements passed to the Producer in order to send a message.
type MessageToSend struct {
	Topic      string
	Key, Value Encoder
}

// ProduceError is the type of error generated when the producer fails to deliver a message.
// It contains the original MessageToSend as well as the actual error value.
type ProduceError struct {
	Msg *MessageToSend
	Err error
}

// Errors is the channel of ProduceErrors for asynchronous delivery.
func (p *Producer) Errors() <-chan *ProduceError {
	return p.errors
}

func (p *Producer) Send() chan<- *MessageToSend {
	return p.input
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

func (p *Producer) receive() {
	for msg := range p.input {
		if msg.Topic == "" {
			p.errors <- &ProduceError{msg, ConfigurationError("Empty topic")}
			continue
		}

		partition, err := p.choosePartition(msg.Topic, msg.Key)
		if err != nil {
			p.errors <- &ProduceError{msg, err}
			continue
		}

		var keyBytes []byte
		var valBytes []byte

		if msg.Key != nil {
			keyBytes, err = msg.Key.Encode()
			if err != nil {
				p.errors <- &ProduceError{msg, err}
				return
			}
		}
		valBytes, err = msg.Value.Encode()
		if err != nil {
			p.errors <- &ProduceError{msg, err}
			return
		}

		p.dispatcher.msgs <- &pendingMessage{msg, partition, keyBytes, valBytes, nil}
	}
	close(p.stopper)
}

// special errors for communication between batcher and dispatcher
// simpler to use an error than to add another field to all pendingMessages
var orderMarker = errors.New("")
var shuttingDown = errors.New("")

type pendingMessage struct {
	orig       *MessageToSend
	partition  int32
	key, value []byte
	err        error
}

type dispatcher struct {
	msgs     chan *pendingMessage
	prod     *Producer
	queues   map[string]map[int32]*msgQueue
	batchers map[*Broker]*batcher
}

type batcher struct {
	prod   *Producer
	broker *Broker
	refs   uint

	msgs   chan *pendingMessage
	owner  map[string]map[int32]bool
	buffer []*pendingMessage

	timer         timer
	bufferedBytes uint

	toRequeue []*pendingMessage
}

type msgQueue struct {
	broker  *Broker
	backlog []*pendingMessage
	requeue []*pendingMessage
}

func (q *msgQueue) flushBacklog() <-chan *pendingMessage {
	msgs := make(chan *pendingMessage)
	go func() {
		for _, msg := range q.requeue {
			msgs <- msg
		}
		for _, msg := range q.backlog {
			msgs <- msg
		}
		q.requeue = nil
		q.backlog = nil
	}()
	return msgs
}

func (d *dispatcher) createBatcher(broker *Broker) {
	var timer timer
	if d.prod.config.MaxBufferTime == 0 {
		timer = &fakeTimer{}
	} else {
		timer = &realTimer{
			time.NewTimer(d.prod.config.MaxBufferTime),
			d.prod.config.MaxBufferTime,
		}
	}

	d.batchers[broker] = &batcher{d.prod, broker, 1,
		make(chan *pendingMessage, 32),
		make(map[string]map[int32]bool),
		nil, timer, 0, nil,
	}

	go d.batchers[broker].processMessages()
}

func (d *dispatcher) getQueue(msg *pendingMessage) *msgQueue {
	if d.queues[msg.orig.Topic] == nil {
		d.queues[msg.orig.Topic] = make(map[int32]*msgQueue)
	}
	if d.queues[msg.orig.Topic][msg.partition] == nil {
		d.queues[msg.orig.Topic][msg.partition] = new(msgQueue)
	}
	return d.queues[msg.orig.Topic][msg.partition]
}

func (d *dispatcher) cleanup() {
	for _, batcher := range d.batchers {
		batcher.msgs <- &pendingMessage{err: shuttingDown}
	}
	waiting := len(d.batchers)
	for msg := range d.msgs {
		switch msg.err {
		case shuttingDown:
			waiting -= 1
			if waiting == 0 {
				close(d.msgs)
				close(d.prod.errors)
			}
		case nil:
			d.prod.errors <- &ProduceError{msg.orig, ConfigurationError("Producer Closed")}
		default:
			d.prod.errors <- &ProduceError{msg.orig, msg.err}
		}
	}
}

func (d *dispatcher) dispatch() {
	for msg := range d.msgs {

		if msg == nil {
			d.cleanup()
			return
		}

		queue := d.getQueue(msg)

		switch msg.err {
		case nil:
			if len(queue.requeue) == 0 {
				if queue.broker == nil {
					var err error
					queue.broker, err = d.prod.client.Leader(msg.orig.Topic, msg.partition)
					if err != nil {
						d.prod.errors <- &ProduceError{msg.orig, err}
						continue
					}
					if d.batchers[queue.broker] != nil {
						d.batchers[queue.broker].refs += 1
					}
				}
				if d.batchers[queue.broker] == nil {
					d.createBatcher(queue.broker)
				}
				d.batchers[queue.broker].msgs <- msg
			} else {
				queue.backlog = append(queue.backlog, msg)
			}
		case orderMarker:
			batcher := d.batchers[queue.broker]
			batcher.refs -= 1
			if batcher.refs == 0 {
				batcher.msgs <- &pendingMessage{err: shuttingDown}
				delete(d.batchers, queue.broker)
			}
			var err error
			queue.broker, err = d.prod.client.Leader(msg.orig.Topic, msg.partition)
			if err != nil {
				for tmp := range queue.flushBacklog() {
					d.prod.errors <- &ProduceError{tmp.orig, err}
				}
				continue
			}
			if d.batchers[queue.broker] == nil {
				d.createBatcher(queue.broker)
			} else {
				d.batchers[queue.broker].refs += 1
			}
			batcher = d.batchers[queue.broker]
			for tmp := range queue.flushBacklog() {
				batcher.msgs <- tmp
			}
		case shuttingDown:
			break // no-op, this is only useful in dispatcher.cleanup()
		default:
			queue.requeue = append(queue.requeue, msg)
			if len(queue.requeue) == 1 {
				// no need to check for nil etc, we just got a message from it so it must exist
				d.batchers[queue.broker].msgs <- &pendingMessage{orig: msg.orig, partition: msg.partition, err: orderMarker}
			}
		}
	}
}

func (b *batcher) buildRequest() *ProduceRequest {

	request := &ProduceRequest{RequiredAcks: b.prod.config.RequiredAcks, Timeout: b.prod.config.Timeout}
	msgs := len(b.buffer)

	if b.prod.config.Compression == CompressionNone {
		for _, msg := range b.buffer {
			request.AddMessage(msg.orig.Topic, msg.partition,
				&Message{Codec: CompressionNone, Key: msg.key, Value: msg.value})
		}
	} else {
		sets := make(map[string]map[int32]*MessageSet)
		for _, msg := range b.buffer {
			if sets[msg.orig.Topic] == nil {
				sets[msg.orig.Topic] = make(map[int32]*MessageSet)
			}
			if sets[msg.orig.Topic][msg.partition] == nil {
				sets[msg.orig.Topic][msg.partition] = new(MessageSet)
			}
			sets[msg.orig.Topic][msg.partition].addMessage(&Message{Codec: CompressionNone, Key: msg.key, Value: msg.value})
		}
		for topic, tmp := range sets {
			for part, set := range tmp {
				bytes, err := encode(set)
				if err == nil {
					request.AddMessage(topic, part,
						&Message{Codec: b.prod.config.Compression, Key: nil, Value: bytes})
				} else {
					for _, msg := range b.buffer {
						if msg.orig.Topic == topic && msg.partition == part {
							msgs -= 1
							b.prod.errors <- &ProduceError{msg.orig, err}
						}
					}
				}
			}
		}
	}

	if msgs > 0 {
		return request
	} else {
		return nil
	}
}

func (b *batcher) redispatch(msg *pendingMessage, err error) {
	if msg.err != nil {
		b.prod.errors <- &ProduceError{msg.orig, err}
	} else {
		msg.err = err
		b.toRequeue = append(b.toRequeue, msg)
		b.owner[msg.orig.Topic][msg.partition] = false
	}
}

func (b *batcher) flush() {
	request := b.buildRequest()

	if request == nil {
		return
	}

	response, err := b.broker.Produce(b.prod.client.id, request)

	switch err {
	case nil:
		if response != nil {
			for _, msg := range b.buffer {
				block := response.GetBlock(msg.orig.Topic, msg.partition)

				if block == nil {
					b.redispatch(msg, IncompleteResponse)
				} else {
					switch block.Err {
					case NoError:
						if b.prod.config.AckSuccesses {
							b.prod.errors <- nil
						}
					case UnknownTopicOrPartition, NotLeaderForPartition, LeaderNotAvailable:
						b.redispatch(msg, err)
					default:
						b.prod.errors <- &ProduceError{msg.orig, err}
					}

				}
			}
		} else if b.prod.config.AckSuccesses {
			for _ = range b.buffer {
				b.prod.errors <- nil
			}
		}
	case EncodingError:
		for _, msg := range b.buffer {
			b.prod.errors <- &ProduceError{msg.orig, err}
		}
	default:
		for _, msg := range b.buffer {
			b.redispatch(msg, err)
		}
	}

	b.buffer = nil
	b.timer.Reset()
}

func (b *batcher) processMessage(msg *pendingMessage) {
	if msg.err == shuttingDown {
		b.flush()
		b.toRequeue = append(b.toRequeue, msg)
		return
	}

	if b.owner[msg.orig.Topic] == nil {
		b.owner[msg.orig.Topic] = make(map[int32]bool)
	}

	if msg.err == orderMarker {
		delete(b.owner[msg.orig.Topic], msg.partition)
		b.toRequeue = append(b.toRequeue, msg)
		return
	}

	_, exists := b.owner[msg.orig.Topic][msg.partition]
	if !exists {
		b.owner[msg.orig.Topic][msg.partition] = true
	}
	if b.owner[msg.orig.Topic][msg.partition] {
		b.buffer = append(b.buffer, msg)
		b.bufferedBytes += uint(len(msg.value))
		if uint(len(b.buffer)) > b.prod.config.MaxBufferedMessages || b.bufferedBytes > b.prod.config.MaxBufferedBytes {
			b.flush()
		}
	} else {
		b.toRequeue = append(b.toRequeue, msg)
	}
}

func (b *batcher) processMessages() {
	for {
		if len(b.toRequeue) == 0 {
			select {
			case msg := <-b.msgs:
				b.processMessage(msg)
			case <-b.timer.C():
				b.flush()
			}
		} else {
			select {
			case b.prod.dispatcher.msgs <- b.toRequeue[0]:
				if b.toRequeue[0].err == shuttingDown {
					close(b.msgs)
					return
				}
				b.toRequeue = b.toRequeue[1:]
			case msg := <-b.msgs:
				b.processMessage(msg)
			case <-b.timer.C():
				b.flush()
			}
		}
	}
}

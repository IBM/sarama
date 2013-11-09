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
	MaxBufferTime       time.Duration    // The maximum amount of time permitted to buffer before flushing.
}

// Producer publishes Kafka messages. It routes messages to the correct broker, refreshing metadata as appropriate,
// and parses responses for errors. You must call Close() on a producer to avoid leaks, it may not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
type Producer struct {
	client     *Client
	dispatcher *dispatcher
	config     ProducerConfig
	errors     chan *ProduceError
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

	prod := &Producer{client, nil, *config, make(chan *ProduceError, 32)}
	prod.dispatcher = &dispatcher{
		make(chan *pendingMessage, 32),
		prod,
		make(map[string]map[int32]*msgQueue),
		make(map[*Broker]*batcher),
	}

	go prod.dispatcher.dispatch()

	return prod, nil
}

// Close shuts down the producer and flushes any messages it may have buffered. You must call this function before
// a producer object passes out of scope, as it may otherwise leak memory. You must call this before calling Close
// on the underlying client.
func (p *Producer) Close() error {
	close(p.dispatcher.msgs)
	return nil
}

// TODO
type ProduceError struct {
	Topic      string
	Key, Value Encoder
	Err        error
}

func (p *Producer) Errors() <-chan *ProduceError {
	return p.errors
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

// SendMessage produces a message on the given topic with the given key and value. The partition to send to is selected
// by the Producer's Partitioner. To send strings as either key or value, see the StringEncoder type.
func (p *Producer) SendMessage(topic string, key, value Encoder) {
	if topic == "" {
		p.errors <- &ProduceError{topic, key, value, ConfigurationError("Empty topic")}
		return
	}

	partition, err := p.choosePartition(topic, key)
	if err != nil {
		p.errors <- &ProduceError{topic, key, value, err}
		return
	}

	var keyBytes []byte
	var valBytes []byte

	if key != nil {
		keyBytes, err = key.Encode()
		if err != nil {
			p.errors <- &ProduceError{topic, key, value, err}
			return
		}
	}
	valBytes, err = value.Encode()
	if err != nil {
		p.errors <- &ProduceError{topic, key, value, err}
		return
	}

	p.dispatcher.msgs <- &pendingMessage{topic, partition, keyBytes, valBytes, key, value, nil}
}

// special errors for communication between batcher and dispatcher
// simpler than adding *another* field to all pendingMessages
var forceFlush = errors.New("")
var flushDone = errors.New("")

type pendingMessage struct {
	topic              string
	partition          int32
	key, value         []byte
	origKey, origValue Encoder
	err                error
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

	msgs    chan *pendingMessage
	timer   timer
	buffers map[string]map[int32][]*pendingMessage

	bufferedBytes uint
	bufferedMsgs  uint
}

type msgQueue struct {
	broker  *Broker
	backlog []*pendingMessage
	requeue []*pendingMessage
}

func (d *dispatcher) createBatcher(broker *Broker) {
	var timer timer
	if d.prod.config.MaxBufferTime == 0 {
		timer = &fakeTimer{make(chan time.Time)}
	} else {
		timer = &realTimer{
			time.NewTimer(d.prod.config.MaxBufferTime),
			d.prod.config.MaxBufferTime,
		}
	}

	d.batchers[broker] = &batcher{d.prod, broker, 1,
		make(chan *pendingMessage, 32),
		timer,
		make(map[string]map[int32][]*pendingMessage),
		0, 0,
	}

	go d.batchers[broker].processMessages()
}

func (d *dispatcher) getQueue(msg *pendingMessage) *msgQueue {
	if d.queues[msg.topic] == nil {
		d.queues[msg.topic] = make(map[int32]*msgQueue)
	}
	if d.queues[msg.topic][msg.partition] == nil {
		d.queues[msg.topic][msg.partition] = new(msgQueue)
	}
	return d.queues[msg.topic][msg.partition]
}

func (d *dispatcher) dispatch() {
	for msg := range d.msgs {
		Logger.Println("dispatcher", msg)

		queue := d.getQueue(msg)

		switch msg.err {
		case nil:
			if len(queue.requeue) == 0 {
				if queue.broker == nil {
					var err error
					queue.broker, err = d.prod.client.Leader(msg.topic, msg.partition)
					if err != nil {
						d.prod.errors <- &ProduceError{msg.topic, msg.origKey, msg.origValue, err}
						continue
					}
					if d.batchers[queue.broker] != nil {
						d.batchers[queue.broker].refs += 1
					}
				}
				if d.batchers[queue.broker] == nil {
					d.createBatcher(queue.broker)
				}
				Logger.Println("dispatching")
				d.batchers[queue.broker].msgs <- msg
			} else {
				queue.backlog = append(queue.backlog, msg)
			}
		case flushDone:
			batcher := d.batchers[queue.broker]
			batcher.refs -= 1
			if batcher.refs == 0 {
				close(batcher.msgs)
				delete(d.batchers, queue.broker)
			}
			var err error
			queue.broker, err = d.prod.client.Leader(msg.topic, msg.partition)
			if err != nil {
				for _, tmp := range queue.requeue {
					d.prod.errors <- &ProduceError{tmp.topic, tmp.origKey, tmp.origValue, err}
				}
				for _, tmp := range queue.backlog {
					d.prod.errors <- &ProduceError{tmp.topic, tmp.origKey, tmp.origValue, err}
				}
				queue.requeue = nil
				queue.backlog = nil
				continue
			}
			if d.batchers[queue.broker] == nil {
				d.createBatcher(queue.broker)
			} else {
				d.batchers[queue.broker].refs += 1
			}
			batcher = d.batchers[queue.broker]
			for _, tmp := range queue.requeue {
				batcher.msgs <- tmp
			}
			for _, tmp := range queue.backlog {
				batcher.msgs <- tmp
			}
			queue.backlog = nil
			queue.requeue = nil
		default:
			queue.requeue = append(queue.requeue, msg)
			if len(queue.requeue) == 1 {
				// no need to check for nil etc, we just got a message from it so it must exist
				d.batchers[queue.broker].msgs <- &pendingMessage{err: forceFlush}
			}
		}
	}
}

func (b *batcher) buildRequest() *ProduceRequest {

	request := &ProduceRequest{RequiredAcks: b.prod.config.RequiredAcks, Timeout: b.prod.config.Timeout}
	msgs := 0

	if b.prod.config.Compression == CompressionNone {
		for _, tmp := range b.buffers {
			for _, buffer := range tmp {
				for _, msg := range buffer {
					msgs += 1
					request.AddMessage(msg.topic, msg.partition,
						&Message{Codec: CompressionNone, Key: msg.key, Value: msg.value})
				}
			}
		}
	} else {
		sets := make(map[string]map[int32]*MessageSet)
		for topic, tmp := range b.buffers {
			for part, buffer := range tmp {
				if sets[topic] == nil {
					sets[topic] = make(map[int32]*MessageSet)
				}
				if sets[topic][part] == nil {
					sets[topic][part] = new(MessageSet)
				}
				for _, msg := range buffer {
					msgs += 1
					sets[topic][part].addMessage(&Message{Codec: CompressionNone, Key: msg.key, Value: msg.value})
				}
			}
		}
		for topic, tmp := range sets {
			for part, set := range tmp {
				bytes, err := encode(set)
				if err == nil {
					request.AddMessage(topic, part,
						&Message{Codec: b.prod.config.Compression, Key: nil, Value: bytes})
				} else {
					for _, msg := range b.buffers[topic][part] {
						msgs -= 1
						b.prod.errors <- &ProduceError{topic, msg.origKey, msg.origValue, err}
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

func (b *batcher) handleError(buf []*pendingMessage, err error) {
	for _, msg := range buf {
		if msg.err != nil {
			b.prod.errors <- &ProduceError{msg.topic, msg.origKey, msg.origValue, err}
		} else {
			msg.err = err
			b.prod.dispatcher.msgs <- msg
		}
	}
}

func (b *batcher) flush() {
	request := b.buildRequest()

	if request == nil {
		return
	}

	Logger.Println("flushing")

	response, err := b.broker.Produce(b.prod.client.id, request)

	switch err {
	case nil:
		break
	case EncodingError:
		for _, tmp := range b.buffers {
			for _, buffer := range tmp {
				for _, msg := range buffer {
					b.prod.errors <- &ProduceError{msg.topic, msg.origKey, msg.origValue, err}
				}
			}
		}
	default:
		for _, tmp := range b.buffers {
			for _, buffer := range tmp {
				b.handleError(buffer, err)
			}
		}
	}

	if response != nil {
		for topic, tmp := range b.buffers {
			for part, buffer := range tmp {
				block := response.GetBlock(topic, part)

				if block == nil {
					b.handleError(buffer, IncompleteResponse)
				} else {
					switch block.Err {
					case NoError:
						break
					case UnknownTopicOrPartition, NotLeaderForPartition, LeaderNotAvailable:
						b.handleError(buffer, err)
					default:
						for _, msg := range buffer {
							b.prod.errors <- &ProduceError{msg.topic, msg.origKey, msg.origValue, err}
						}
					}

				}
			}
		}
	}

	b.buffers = make(map[string]map[int32][]*pendingMessage)
	b.timer.Reset()
}

func (b *batcher) processMessages() {
	for {
		select {
		case msg := <-b.msgs:
			Logger.Println("batcher", msg)

			if msg == nil {
				b.flush()
				Logger.Println("batcher exiting")
				return
			}

			if b.buffers[msg.topic] == nil {
				b.buffers[msg.topic] = make(map[int32][]*pendingMessage)
			}
			_, exists := b.buffers[msg.topic][msg.partition]
			if !exists {
				b.buffers[msg.topic][msg.partition] = make([]*pendingMessage, 0)
			}
			if b.buffers[msg.topic][msg.partition] != nil {
				b.buffers[msg.topic][msg.partition] = append(b.buffers[msg.topic][msg.partition], msg)
				b.bufferedMsgs += 1
				b.bufferedBytes += uint(len(msg.value))
				if b.bufferedMsgs > b.prod.config.MaxBufferedMessages || b.bufferedBytes > b.prod.config.MaxBufferedBytes {
					b.flush()
				}
			} else {
				b.prod.dispatcher.msgs <- msg
			}
		case <-b.timer.C():
			b.flush()
		}
	}
}

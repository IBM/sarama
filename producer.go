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
	MaxBufferTime       uint             // The maximum amount of time (in milliseconds) permitted to buffer before flushing.
}

// Producer publishes Kafka messages. It routes messages to the correct broker, refreshing metadata as appropriate,
// and parses responses for errors. You must call Close() on a producer to avoid leaks, it may not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
type Producer struct {
	client *Client
	config ProducerConfig
	msgs   chan *pendingMessage
	events chan error
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

	p := &Producer{client, *config, make(chan *pendingMessage), make(chan error)}

	go p.dispatcher()

	return p, nil
}

// Close shuts down the producer and flushes any messages it may have buffered. You must call this function before
// a producer object passes out of scope, as it may otherwise leak memory. You must call this before calling Close
// on the underlying client.
func (p *Producer) Close() error {
	close(p.msgs)
	return nil
}

func (p *Producer) Events() <-chan error {
	return p.events
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
func (p *Producer) SendMessage(topic string, key, value Encoder) error {
	if topic == "" {
		return ConfigurationError("Empty topic")
	}

	partition, err := p.choosePartition(topic, key)
	if err != nil {
		return err
	}

	var keyBytes []byte
	var valBytes []byte

	if key != nil {
		keyBytes, err = key.Encode()
		if err != nil {
			return err
		}
	}
	valBytes, err = value.Encode()
	if err != nil {
		return err
	}

	p.msgs <- &pendingMessage{topic, partition, keyBytes, valBytes, nil}

	return nil
}

func (p *Producer) safeSendMessage(key, value Encoder, retry bool) error {

	response, err := broker.Produce(p.client.id, request)
	switch err {
	case nil:
		break
	case EncodingError:
		return err
	default:
		if !retry {
			return err
		}
		p.client.disconnectBroker(broker)
		return p.safeSendMessage(key, value, false)
	}

	if response == nil {
		return nil
	}

	block := response.GetBlock(p.topic, partition)
	if block == nil {
		return IncompleteResponse
	}

	switch block.Err {
	case NoError:
		return nil
	case UnknownTopicOrPartition, NotLeaderForPartition, LeaderNotAvailable:
		if !retry {
			return block.Err
		}
		err = p.client.RefreshTopicMetadata(p.topic)
		if err != nil {
			return err
		}
		return p.safeSendMessage(key, value, false)
	}

	return block.Err
}

// special errors for communication between batcher and dispatcher
// simpler than adding *another* field to all pendingMessages
var forceFlush = errors.New("")
var flushDone = errors.New("")

type pendingMessage struct {
	topic      string
	partition  int32
	key, value []byte
	err        error
}

type batcherHandle struct {
	msgs chan *pendingMessage
	refs uint
}

type msgQueue struct {
	broker  *Broker
	backlog []*pendingMessage
	requeue []*pendingMessage
}

func (p *Producer) dispatcher() {
	queues := make(map[string]map[int32]*msgQueue)
	batchers := make(map[*Broker]*batcherHandle)

	for msg := range p.msgs {
		if queues[msg.topic] == nil {
			queues[msg.topic] = make(map[int32]*msgQueue)
		}
		if queues[msg.topic][msg.partition] == nil {
			queues[msg.topic][msg.partition] = new(msgQueue)
		}
		queue := queues[msg.topic][msg.partition]

		switch msg.err {
		case nil:
			if len(queue.requeue) == 0 {
				if queue.broker == nil {
					var err error
					queue.broker, err = p.client.Leader(msg.topic, msg.partition)
					if err != nil {
						p.events <- err
						continue
					}
					if batchers[queue.broker] != nil {
						batchers[queue.broker].refs += 1
					}
				}
				if batchers[queue.broker] == nil {
					batcherChan := make(chan *pendingMessage)
					batchers[queue.broker] = &batcherHandle{batcherChan, 1}
					go p.batcher(queue.broker, batcherChan)
				}
				batchers[queue.broker].msgs <- msg
			} else {
				queue.backlog = append(queue.backlog, msg)
			}
		case flushDone:
			batcher := batchers[queue.broker]
			batcher.refs -= 1
			if batcher.refs == 0 {
				close(batcher.msgs)
				delete(batchers, queue.broker)
			}
			var err error
			queue.broker, err = p.client.Leader(msg.topic, msg.partition)
			if err != nil {
				p.events <- err
				queue.backlog = nil
				queue.requeue = nil
				continue
			}
			if batchers[queue.broker] == nil {
				batcherChan := make(chan *pendingMessage)
				batchers[queue.broker] = &batcherHandle{batcherChan, 1}
				go p.batcher(queue.broker, batcherChan)
			} else {
				batchers[queue.broker].refs += 1
			}
			batcher = batchers[queue.broker]
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
				batchers[queue.broker].msgs <- &pendingMessage{err: forceFlush}
			}
		}
	}
}

func (p *Producer) buildProduceRequest(msgs map[string]map[int32][]*pendingMessage) (*ProduceRequest, error) {

	request := &ProduceRequest{RequiredAcks: p.config.RequiredAcks, Timeout: p.config.Timeout}

	if p.config.Compression == CompressionNone {
		for _, tmp := range msgs {
			for _, buffer := range tmp {
				for _, msg := range buffer {
					request.AddMessage(msg.topic, msg.partition,
						&Message{Codec: CompressionNone, Key: msg.key, Value: msg.value})
				}
			}
		}
	} else {
		sets := make(map[string]map[int32]*MessageSet)
		for topic, tmp := range msgs {
			for part, buffer := range tmp {
				if sets[topic] == nil {
					sets[topic] = make(map[int32]*MessageSet)
				}
				if sets[topic][part] == nil {
					sets[topic][part] = new(MessageSet)
				}
				for _, msg := range buffer {
					sets[topic][part].addMessage(&Message{Codec: CompressionNone, Key: msg.key, Value: msg.value})
				}
			}
		}
		for topic, tmp := range sets {
			for part, set := range tmp {
				bytes, err := encode(set)
				if err != nil {
					return nil, err
				}
				request.AddMessage(topic, part,
					&Message{Codec: p.config.Compression, Key: nil, Value: bytes})
			}
		}
	}

	return request, nil
}

func (p *Producer) batcher(broker *Broker, msgs <-chan *pendingMessage) {
	buffers := make(map[string]map[int32][]*pendingMessage)
	timeout := time.Duration(p.config.MaxBufferTime) * time.Millisecond
	timer := time.NewTimer(timeout)

	var bufferedBytes uint
	var bufferedMsgs uint

	for {
		select {
		case msg := <-msgs:
			if buffers[msg.topic] == nil {
				buffers[msg.topic] = make(map[int32][]*pendingMessage)
			}
			buffer, exists := buffers[msg.topic][msg.partition]
			if !exists {
				buffers[msg.topic][msg.partition] = nil
			}
			if buffer != nil {
				buffers[msg.topic][msg.partition] = append(buffers[msg.topic][msg.partition], msg)
				bufferedMsgs += 1
				bufferedBytes += uint(len(msg.value))
				if bufferedMsgs > p.config.MaxBufferedMessages || bufferedBytes > p.config.MaxBufferedBytes {
					request, err := p.buildProduceRequest(buffers)        // TODO handle err
					response, err := broker.Produce(p.client.id, request) // TODO handle err
					// TODO check response
					timer.Reset(timeout)
				}
			} else {
				p.msgs <- msg
			}
		case <-timer.C:
			request, err := p.buildProduceRequest(buffers)        // TODO handle err
			response, err := broker.Produce(p.client.id, request) // TODO handle err
			// TODO check response
			timer.Reset(timeout)
		}
	}
}

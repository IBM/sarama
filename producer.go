package sarama

import (
	"errors"
	"time"
)

// ProducerConfig is used to pass multiple configuration options to NewProducer.
type ProducerConfig struct {
	Partitioners       map[string]Partitioner // Map of topics to partitioners. If a topic does not exist in the map, DefaultPartitioner is used.
	DefaultPartitioner Partitioner            // Default method of choosing the partition to send messages to. Random is used if this is nil.

	RequiredAcks RequiredAcks     // The level of acknowledgement reliability needed from the broker (defaults to no acknowledgement).
	Timeout      int32            // The maximum time in ms the broker will wait for the receipt of the number of RequiredAcks.
	Compression  CompressionCodec // The type of compression to use on messages (defaults to no compression).

	MaxBufferedMessages uint          // The maximum number of messages permitted to buffer before flushing.
	MaxBufferedBytes    uint          // The maximum number of message bytes permitted to buffer before flushing (before compression).
	MaxBufferTime       time.Duration // The maximum amount of time permitted to buffer before flushing (or zero for no timer).

	// When true, every message sent results in exactly one ProduceError on the Errors() channel,
	// even if the message was delivered successfully (in which case the Err field will be nil).
	AckSuccesses bool
}

// Producer publishes Kafka messages. It routes messages to the correct broker, refreshing metadata as appropriate,
// and parses responses for errors. You must call Close() on a producer to avoid leaks, it will not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close() on the underlying client, which is still necessary). The
// Producer is fully asynchronous to enable maximum throughput.
type Producer struct {
	client     *Client
	dispatcher *dispatcher
	config     ProducerConfig
	errors     chan *ProduceError  // errors get returned to the user here
	input      chan *MessageToSend // messages get sent from the user here
	stopper    chan bool           // for the messagePreprocessor
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

	if config.DefaultPartitioner == nil {
		config.DefaultPartitioner = NewRandomPartitioner()
	}

	prod := &Producer{client, nil, *config,
		make(chan *ProduceError, efficientBufferSize),
		make(chan *MessageToSend, efficientBufferSize),
		make(chan bool),
	}
	prod.dispatcher = &dispatcher{
		make(chan *pendingMessage, efficientBufferSize),
		prod,
		make(map[string]map[int32]*msgQueue),
		make(map[*Broker]*batcher),
	}

	go withRecover(prod.messagePreprocessor)
	go withRecover(prod.dispatcher.dispatch)

	return prod, nil
}

// Close shuts down the producer and flushes any messages it may have buffered. You must call this function before
// a producer object passes out of scope, as it may otherwise leak memory. You must call this before calling Close
// on the underlying client. It is an error to put messages on the Send() channel after calling Close. Errors may still
// appear on the Errors() channel after calling close, until the shutdown procedure is finished, at which point the Errors()
// channel itself will be closed.
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
// It contains the original MessageToSend as well as the actual error value. If the AckSuccesses configuration
// value is set to true then every message sent generates a ProduceError, but successes will have a nil Err field.
type ProduceError struct {
	Msg *MessageToSend
	Err error
}

// Errors is the output channel back to the user. If you do not read from this channel,
// the Producer may deadlock. It is suggested that you send messages and read errors in a select statement.
func (p *Producer) Errors() <-chan *ProduceError {
	return p.errors
}

// Send is the input channel for the user to write to.
func (p *Producer) Send() chan<- *MessageToSend {
	return p.input
}

func (p *Producer) choosePartition(topic string, key Encoder) (int32, error) {
	partitions, err := p.client.Partitions(topic)
	if err != nil {
		return -1, err
	}

	numPartitions := int32(len(partitions))

	chooser := p.config.Partitioners[topic]
	if chooser == nil {
		chooser = p.config.DefaultPartitioner
	}

	choice := chooser.Partition(key, numPartitions)

	if choice < 0 || choice >= numPartitions {
		return -1, InvalidPartition
	}

	return partitions[choice], nil
}

// used to store necessary extra data with each MessageToSend
type pendingMessage struct {
	orig       *MessageToSend
	partition  int32
	key, value []byte
	err        error
}

// messagePreprocessor does the work necessary to convert MessageToSend structs (as provided by the user)
// into the pendingMessage structs expected by the dispatcher. This includes choosing a partition etc.
func (p *Producer) messagePreprocessor() {
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
var (
	bounced  = errors.New("message was rejected by the batcher without being sent as it no longer owns the relevant topic/partition")
	reset    = errors.New("leader of topic/partition is changing")
	shutdown = errors.New("shutting down batcher")
)

// each topic/partition has an associated message queue
type msgQueue struct {
	broker *Broker

	// in normal operation these are both empty (new messages are passed directly to the batcher,
	// as identified by the broker), but when a delivery fails (e.g. due to leader election)
	// then we store messages here until we can guarantee ordering again
	backlog []*pendingMessage // new messages from the user
	requeue []*pendingMessage // messages that have failed once already
}

// helper to stitch the two queues together into a single queue in the correct order
func (q *msgQueue) flushBacklog() <-chan *pendingMessage {
	msgs := make(chan *pendingMessage)
	go withRecover(func() {
		for _, msg := range q.requeue {
			msgs <- msg
		}
		for _, msg := range q.backlog {
			msgs <- msg
		}
		q.requeue = nil
		q.backlog = nil
	})
	return msgs
}

// responsible for taking messages and sending them to the correct batcher
// also responsible for creating/destroying batchers as necessary
type dispatcher struct {
	msgs     chan *pendingMessage // input to the dispatcher
	prod     *Producer
	queues   map[string]map[int32]*msgQueue // dispatches according to these
	batchers map[*Broker]*batcher           // dispatches to these
}

// responsible for collecting messages destined for a single broker and batching them together
// messages that fail are returned to the user or to the dispatcher, depending on the severity of
// the error and if that message has failed before
type batcher struct {
	prod   *Producer
	broker *Broker
	refs   uint // how many topic/partitions on this broker, used so the dispatcher knows when to clean up the batcher

	msgs   chan *pendingMessage      // input from the dispatcher
	owner  map[string]map[int32]bool // whether I own the given topic/partitions
	buffer []*pendingMessage         // buffer of messages to send

	// flush-condition variables, len(buffer) is also used
	timer         timer
	bufferedBytes uint

	// messages to return to the dispatcher, which is done in a select{} with receiving on msgs
	// in order to avoid deadlocks when sending/receiveing at the same time
	toRequeue []*pendingMessage
}

// helper to create a new batcher
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
		make(chan *pendingMessage, efficientBufferSize),
		make(map[string]map[int32]bool),
		nil, timer, 0, nil,
	}

	go withRecover(d.batchers[broker].processMessages)
}

// helper to get the appropriate msgQueue, or create it if it doesn't exist
func (d *dispatcher) getQueue(msg *pendingMessage) *msgQueue {
	if d.queues[msg.orig.Topic] == nil {
		d.queues[msg.orig.Topic] = make(map[int32]*msgQueue)
	}
	if d.queues[msg.orig.Topic][msg.partition] == nil {
		d.queues[msg.orig.Topic][msg.partition] = new(msgQueue)
	}
	return d.queues[msg.orig.Topic][msg.partition]
}

// shutdown dispatcher. sends shutdown to all batchers, then waits for that many 'shutdown' responses before exiting
// any non-shutdown messages are sent back to the user, even if they would otherwise be retried
func (d *dispatcher) cleanup() {
	for _, batcher := range d.batchers {
		batcher.msgs <- &pendingMessage{err: shutdown}
	}
	waiting := len(d.batchers)
	for msg := range d.msgs {
		switch msg.err {
		case shutdown:
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

// main dispatcher goroutine
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
				// no requeue means we're in a normal state, so just send to the batcher (creating it if necessary)
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
				// we have a requeue, so just append to the user-backlog for now
				queue.backlog = append(queue.backlog, msg)
			}
		case reset:
			// we got a reset *back* from the batcher, which means we've received all the failed messages
			// on this topic-partition, which means we can flush the backlog and guarantee we'll get the
			// order right
			batcher := d.batchers[queue.broker]
			batcher.refs -= 1
			if batcher.refs == 0 {
				batcher.msgs <- &pendingMessage{err: shutdown}
				delete(d.batchers, queue.broker)
			}
			var err error
			err = d.prod.client.RefreshTopicMetadata(msg.orig.Topic)
			if err == nil {
				queue.broker, err = d.prod.client.Leader(msg.orig.Topic, msg.partition)
			}
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
		case shutdown:
			break // no-op, this is only useful in dispatcher.cleanup()
		case bounced:
			msg.err = nil
			fallthrough
		default:
			// a retryable error from the batcher, so add it to the requeue
			queue.requeue = append(queue.requeue, msg)
			if len(queue.requeue) == 1 {
				// first requeue, so send a reset to let the batcher know that we're redirecting this topic/partition away
				// from them. no need to check for nil etc, we just got a message from it so it must exist
				d.batchers[queue.broker].msgs <- &pendingMessage{orig: msg.orig, partition: msg.partition, err: reset}
			}
		}
	}
}

// helper to build a produceRequest from the buffer of messages
// this is non-trivially different depending on whether compression is requested or not
func (b *batcher) buildRequest() *ProduceRequest {

	request := &ProduceRequest{RequiredAcks: b.prod.config.RequiredAcks, Timeout: b.prod.config.Timeout}
	msgs := len(b.buffer)

	if b.prod.config.Compression == CompressionNone {
		// no compression, just each message to the request
		for _, msg := range b.buffer {
			request.AddMessage(msg.orig.Topic, msg.partition,
				&Message{Codec: CompressionNone, Key: msg.key, Value: msg.value})
		}
	} else {
		// compression requested. Build a MessageSet for each topic/partition combination,
		// then add messages to the appropriate MessageSet
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
		// now compress each MessageSet into a single "message" at the actual protocol level. kafka is weird
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
		// if the message has been retried before, send it back to the user
		b.prod.errors <- &ProduceError{msg.orig, err}
	} else {
		// otherwise send it back to the dispatcher, appropriately marked, and note
		// that we no longer own this topic/partition
		msg.err = err
		b.toRequeue = append(b.toRequeue, msg)
		b.owner[msg.orig.Topic][msg.partition] = false
	}
}

// helper to flush the current buffer to the broker
func (b *batcher) flush() {
	request := b.buildRequest()

	if request == nil {
		return
	}

	response, err := b.broker.Produce(b.prod.client.id, request)

	switch err {
	case nil:
		if response != nil {
			// we got a response, check it
			for _, msg := range b.buffer {
				block := response.GetBlock(msg.orig.Topic, msg.partition)

				if block == nil {
					b.redispatch(msg, IncompleteResponse)
				} else {
					switch block.Err {
					case NoError:
						if b.prod.config.AckSuccesses {
							b.prod.errors <- &ProduceError{msg.orig, nil}
						}
					case UnknownTopicOrPartition, NotLeaderForPartition, LeaderNotAvailable:
						b.redispatch(msg, err)
					default:
						b.prod.errors <- &ProduceError{msg.orig, err}
					}

				}
			}
		} else if b.prod.config.AckSuccesses {
			// no response, assume success
			for _, msg := range b.buffer {
				b.prod.errors <- &ProduceError{msg.orig, nil}
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
	if msg.err == shutdown {
		b.flush()
		// we have to let the dispatcher know we've exited before we can close our channel
		b.toRequeue = append(b.toRequeue, msg)
		return
	}

	if b.owner[msg.orig.Topic] == nil {
		b.owner[msg.orig.Topic] = make(map[int32]bool)
	}

	if msg.err == reset {
		delete(b.owner[msg.orig.Topic], msg.partition)
		// pass the reset back so the dispatcher nows we've flushed all the problem packets back to it
		b.toRequeue = append(b.toRequeue, msg)
		return
	}

	_, exists := b.owner[msg.orig.Topic][msg.partition]
	if !exists {
		// never seen this topic/partition before, trust the dispatcher that we must own it
		b.owner[msg.orig.Topic][msg.partition] = true
	}
	if b.owner[msg.orig.Topic][msg.partition] {
		b.buffer = append(b.buffer, msg)
		b.bufferedBytes += uint(len(msg.value))
		conf := b.prod.config
		// flush if all three are zero (indicating message-at-a-time) or if either of our two
		// message-dependent conditions are both non-zero and met
		if (conf.MaxBufferedMessages == 0 && conf.MaxBufferedBytes == 0 && conf.MaxBufferTime == 0) ||
			(conf.MaxBufferedMessages > 0 && uint(len(b.buffer)) > conf.MaxBufferedMessages) ||
			(conf.MaxBufferedBytes > 0 && b.bufferedBytes > conf.MaxBufferedMessages) {

			b.flush()
		}
	} else {
		// we do *not* own this topic/partition anymore, bounce it back
		if msg.err == nil {
			msg.err = bounced
		}
		b.toRequeue = append(b.toRequeue, msg)
	}
}

// main batcher goroutine
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
				if b.toRequeue[0].err == shutdown {
					// if we just sent the dispatcher a shutdown notification, we're done, so
					// close the channel and exit the goroutine
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

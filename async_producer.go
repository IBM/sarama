package sarama

import (
	"fmt"
	"sync"
	"time"

	"github.com/eapache/go-resiliency/breaker"
	"github.com/eapache/queue"
)

// AsyncProducer publishes Kafka messages using a non-blocking API. It routes messages
// to the correct broker for the provided topic-partition, refreshing metadata as appropriate,
// and parses responses for errors. You must read from the Errors() channel or the
// producer will deadlock. You must call Close() or AsyncClose() on a producer to avoid
// leaks: it will not be garbage-collected automatically when it passes out of
// scope.
type AsyncProducer interface {

	// AsyncClose triggers a shutdown of the producer, flushing any messages it may
	// have buffered. The shutdown has completed when both the Errors and Successes
	// channels have been closed. When calling AsyncClose, you *must* continue to
	// read from those channels in order to drain the results of any messages in
	// flight.
	AsyncClose()

	// Close shuts down the producer and flushes any messages it may have buffered.
	// You must call this function before a producer object passes out of scope, as
	// it may otherwise leak memory. You must call this before calling Close on the
	// underlying client.
	Close() error

	// Input is the input channel for the user to write messages to that they
	// wish to send.
	Input() chan<- *ProducerMessage

	// Successes is the success output channel back to the user when AckSuccesses is
	// enabled. If Return.Successes is true, you MUST read from this channel or the
	// Producer will deadlock. It is suggested that you send and read messages
	// together in a single select statement.
	Successes() <-chan *ProducerMessage

	// Errors is the error output channel back to the user. You MUST read from this
	// channel or the Producer will deadlock when the channel is full. Alternatively,
	// you can set Producer.Return.Errors in your config to false, which prevents
	// errors to be returned.
	Errors() <-chan *ProducerError
}

type asyncProducer struct {
	client    Client
	conf      *Config
	ownClient bool

	errors                    chan *ProducerError
	input, successes, retries chan *ProducerMessage
	inFlight                  sync.WaitGroup

	brokers    map[*Broker]chan<- *ProducerMessage
	brokerRefs map[chan<- *ProducerMessage]int
	brokerLock sync.Mutex
}

// NewAsyncProducer creates a new AsyncProducer using the given broker addresses and configuration.
func NewAsyncProducer(addrs []string, conf *Config) (AsyncProducer, error) {
	client, err := NewClient(addrs, conf)
	if err != nil {
		return nil, err
	}

	p, err := NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}
	p.(*asyncProducer).ownClient = true
	return p, nil
}

// NewAsyncProducerFromClient creates a new Producer using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this producer.
func NewAsyncProducerFromClient(client Client) (AsyncProducer, error) {
	// Check that we are not dealing with a closed Client before processing any other arguments
	if client.Closed() {
		return nil, ErrClosedClient
	}

	p := &asyncProducer{
		client:     client,
		conf:       client.Config(),
		errors:     make(chan *ProducerError),
		input:      make(chan *ProducerMessage),
		successes:  make(chan *ProducerMessage),
		retries:    make(chan *ProducerMessage),
		brokers:    make(map[*Broker]chan<- *ProducerMessage),
		brokerRefs: make(map[chan<- *ProducerMessage]int),
	}

	// launch our singleton dispatchers
	go withRecover(p.dispatcher)
	go withRecover(p.retryHandler)

	return p, nil
}

type flagSet int8

const (
	chaser   flagSet = 1 << iota // message is last in a group that failed
	shutdown                     // start the shutdown process
)

// ProducerMessage is the collection of elements passed to the Producer in order to send a message.
type ProducerMessage struct {
	Topic string // The Kafka topic for this message.
	// The partitioning key for this message. Pre-existing Encoders include
	// StringEncoder and ByteEncoder.
	Key Encoder
	// The actual message to store in Kafka. Pre-existing Encoders include
	// StringEncoder and ByteEncoder.
	Value Encoder

	// This field is used to hold arbitrary data you wish to include so it
	// will be available when receiving on the Successes and Errors channels.
	// Sarama completely ignores this field and is only to be used for
	// pass-through data.
	Metadata interface{}

	// Below this point are filled in by the producer as the message is processed

	// Offset is the offset of the message stored on the broker. This is only
	// guaranteed to be defined if the message was successfully delivered and
	// RequiredAcks is not NoResponse.
	Offset int64
	// Partition is the partition that the message was sent to. This is only
	// guaranteed to be defined if the message was successfully delivered.
	Partition int32

	retries int
	flags   flagSet
}

const producerMessageOverhead = 26 // the metadata overhead of CRC, flags, etc.

func (m *ProducerMessage) byteSize() int {
	size := producerMessageOverhead
	if m.Key != nil {
		size += m.Key.Length()
	}
	if m.Value != nil {
		size += m.Value.Length()
	}
	return size
}

func (m *ProducerMessage) clear() {
	m.flags = 0
	m.retries = 0
}

// ProducerError is the type of error generated when the producer fails to deliver a message.
// It contains the original ProducerMessage as well as the actual error value.
type ProducerError struct {
	Msg *ProducerMessage
	Err error
}

func (pe ProducerError) Error() string {
	return fmt.Sprintf("kafka: Failed to produce message to topic %s: %s", pe.Msg.Topic, pe.Err)
}

// ProducerErrors is a type that wraps a batch of "ProducerError"s and implements the Error interface.
// It can be returned from the Producer's Close method to avoid the need to manually drain the Errors channel
// when closing a producer.
type ProducerErrors []*ProducerError

func (pe ProducerErrors) Error() string {
	return fmt.Sprintf("kafka: Failed to deliver %d messages.", len(pe))
}

func (p *asyncProducer) Errors() <-chan *ProducerError {
	return p.errors
}

func (p *asyncProducer) Successes() <-chan *ProducerMessage {
	return p.successes
}

func (p *asyncProducer) Input() chan<- *ProducerMessage {
	return p.input
}

func (p *asyncProducer) Close() error {
	p.AsyncClose()

	if p.conf.Producer.Return.Successes {
		go withRecover(func() {
			for _ = range p.successes {
			}
		})
	}

	var errors ProducerErrors
	if p.conf.Producer.Return.Errors {
		for event := range p.errors {
			errors = append(errors, event)
		}
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

func (p *asyncProducer) AsyncClose() {
	go withRecover(p.shutdown)
}

// singleton
// dispatches messages by topic
func (p *asyncProducer) dispatcher() {
	handlers := make(map[string]chan<- *ProducerMessage)
	shuttingDown := false

	for msg := range p.input {
		if msg == nil {
			Logger.Println("Something tried to send a nil message, it was ignored.")
			continue
		}

		if msg.flags&shutdown != 0 {
			shuttingDown = true
			p.inFlight.Done()
			continue
		} else if msg.retries == 0 {
			if shuttingDown {
				// we can't just call returnError here because that decrements the wait group,
				// which hasn't been incremented yet for this message, and shouldn't be
				pErr := &ProducerError{Msg: msg, Err: ErrShuttingDown}
				if p.conf.Producer.Return.Errors {
					p.errors <- pErr
				} else {
					Logger.Println(pErr)
				}
				continue
			}
			p.inFlight.Add(1)
		}

		if (p.conf.Producer.Compression == CompressionNone && msg.Value != nil && msg.Value.Length() > p.conf.Producer.MaxMessageBytes) ||
			(msg.byteSize() > p.conf.Producer.MaxMessageBytes) {

			p.returnError(msg, ErrMessageSizeTooLarge)
			continue
		}

		handler := handlers[msg.Topic]
		if handler == nil {
			handler = p.newTopicProducer(msg.Topic)
			handlers[msg.Topic] = handler
		}

		handler <- msg
	}

	for _, handler := range handlers {
		close(handler)
	}
}

// one per topic
// partitions messages, then dispatches them by partition
type topicProducer struct {
	parent *asyncProducer
	topic  string
	input  <-chan *ProducerMessage

	breaker     *breaker.Breaker
	handlers    map[int32]chan<- *ProducerMessage
	partitioner Partitioner
}

func (p *asyncProducer) newTopicProducer(topic string) chan<- *ProducerMessage {
	input := make(chan *ProducerMessage, p.conf.ChannelBufferSize)
	tp := &topicProducer{
		parent:      p,
		topic:       topic,
		input:       input,
		breaker:     breaker.New(3, 1, 10*time.Second),
		handlers:    make(map[int32]chan<- *ProducerMessage),
		partitioner: p.conf.Producer.Partitioner(topic),
	}
	go withRecover(tp.dispatch)
	return input
}

func (tp *topicProducer) dispatch() {
	for msg := range tp.input {
		if msg.retries == 0 {
			if err := tp.partitionMessage(msg); err != nil {
				tp.parent.returnError(msg, err)
				continue
			}
		}

		handler := tp.handlers[msg.Partition]
		if handler == nil {
			handler = tp.parent.newPartitionProducer(msg.Topic, msg.Partition)
			tp.handlers[msg.Partition] = handler
		}

		handler <- msg
	}

	for _, handler := range tp.handlers {
		close(handler)
	}
}

func (tp *topicProducer) partitionMessage(msg *ProducerMessage) error {
	var partitions []int32

	err := tp.breaker.Run(func() (err error) {
		if tp.partitioner.RequiresConsistency() {
			partitions, err = tp.parent.client.Partitions(msg.Topic)
		} else {
			partitions, err = tp.parent.client.WritablePartitions(msg.Topic)
		}
		return
	})

	if err != nil {
		return err
	}

	numPartitions := int32(len(partitions))

	if numPartitions == 0 {
		return ErrLeaderNotAvailable
	}

	choice, err := tp.partitioner.Partition(msg, numPartitions)

	if err != nil {
		return err
	} else if choice < 0 || choice >= numPartitions {
		return ErrInvalidPartition
	}

	msg.Partition = partitions[choice]

	return nil
}

// one per partition per topic
// dispatches messages to the appropriate broker
// also responsible for maintaining message order during retries
type partitionProducer struct {
	parent    *asyncProducer
	topic     string
	partition int32
	input     <-chan *ProducerMessage

	leader  *Broker
	breaker *breaker.Breaker
	output  chan<- *ProducerMessage

	// highWatermark tracks the "current" retry level, which is the only one where we actually let messages through,
	// all other messages get buffered in retryState[msg.retries].buf to preserve ordering
	// retryState[msg.retries].expectChaser simply tracks whether we've seen a chaser message for a given level (and
	// therefore whether our buffer is complete and safe to flush)
	highWatermark int
	retryState    []partitionRetryState
}

type partitionRetryState struct {
	buf          []*ProducerMessage
	expectChaser bool
}

func (p *asyncProducer) newPartitionProducer(topic string, partition int32) chan<- *ProducerMessage {
	input := make(chan *ProducerMessage, p.conf.ChannelBufferSize)
	pp := &partitionProducer{
		parent:    p,
		topic:     topic,
		partition: partition,
		input:     input,

		breaker:    breaker.New(3, 1, 10*time.Second),
		retryState: make([]partitionRetryState, p.conf.Producer.Retry.Max+1),
	}
	go withRecover(pp.dispatch)
	return input
}

func (pp *partitionProducer) dispatch() {
	// try to prefetch the leader; if this doesn't work, we'll do a proper call to `updateLeader`
	// on the first message
	pp.leader, _ = pp.parent.client.Leader(pp.topic, pp.partition)
	if pp.leader != nil {
		pp.output = pp.parent.getBrokerProducer(pp.leader)
	}

	for msg := range pp.input {
		if msg.retries > pp.highWatermark {
			// a new, higher, retry level; handle it and then back off
			pp.newHighWatermark(msg.retries)
			time.Sleep(pp.parent.conf.Producer.Retry.Backoff)
		} else if pp.highWatermark > 0 {
			// we are retrying something (else highWatermark would be 0) but this message is not a *new* retry level
			if msg.retries < pp.highWatermark {
				// in fact this message is not even the current retry level, so buffer it for now (unless it's a just a chaser)
				if msg.flags&chaser == chaser {
					pp.retryState[msg.retries].expectChaser = false
					pp.parent.inFlight.Done() // this chaser is now handled and will be garbage collected
				} else {
					pp.retryState[msg.retries].buf = append(pp.retryState[msg.retries].buf, msg)
				}
				continue
			} else if msg.flags&chaser == chaser {
				// this message is of the current retry level (msg.retries == highWatermark) and the chaser flag is set,
				// meaning this retry level is done and we can go down (at least) one level and flush that
				pp.retryState[pp.highWatermark].expectChaser = false
				pp.flushRetryBuffers()
				pp.parent.inFlight.Done() // this chaser is now handled and will be garbage collected
				continue
			}
		}

		// if we made it this far then the current msg contains real data, and can be sent to the next goroutine
		// without breaking any of our ordering guarantees

		if pp.output == nil {
			if err := pp.updateLeader(); err != nil {
				pp.parent.returnError(msg, err)
				time.Sleep(pp.parent.conf.Producer.Retry.Backoff)
				continue
			}
			Logger.Printf("producer/leader/%s/%d selected broker %d\n", pp.topic, pp.partition, pp.leader.ID())
		}

		pp.output <- msg
	}

	if pp.output != nil {
		pp.parent.unrefBrokerProducer(pp.leader, pp.output)
	}
}

func (pp *partitionProducer) newHighWatermark(hwm int) {
	Logger.Printf("producer/leader/%s/%d state change to [retrying-%d]\n", pp.topic, pp.partition, hwm)
	pp.highWatermark = hwm

	// send off a chaser so that we know when everything "in between" has made it
	// back to us and we can safely flush the backlog (otherwise we risk re-ordering messages)
	pp.retryState[pp.highWatermark].expectChaser = true
	pp.parent.inFlight.Add(1) // we're generating a chaser message; track it so we don't shut down while it's still inflight
	pp.output <- &ProducerMessage{Topic: pp.topic, Partition: pp.partition, flags: chaser, retries: pp.highWatermark - 1}

	// a new HWM means that our current broker selection is out of date
	Logger.Printf("producer/leader/%s/%d abandoning broker %d\n", pp.topic, pp.partition, pp.leader.ID())
	pp.parent.unrefBrokerProducer(pp.leader, pp.output)
	pp.output = nil
}

func (pp *partitionProducer) flushRetryBuffers() {
	Logger.Printf("producer/leader/%s/%d state change to [flushing-%d]\n", pp.topic, pp.partition, pp.highWatermark)
	for {
		pp.highWatermark--

		if pp.output == nil {
			if err := pp.updateLeader(); err != nil {
				pp.parent.returnErrors(pp.retryState[pp.highWatermark].buf, err)
				goto flushDone
			}
			Logger.Printf("producer/leader/%s/%d selected broker %d\n", pp.topic, pp.partition, pp.leader.ID())
		}

		for _, msg := range pp.retryState[pp.highWatermark].buf {
			pp.output <- msg
		}

	flushDone:
		pp.retryState[pp.highWatermark].buf = nil
		if pp.retryState[pp.highWatermark].expectChaser {
			Logger.Printf("producer/leader/%s/%d state change to [retrying-%d]\n", pp.topic, pp.partition, pp.highWatermark)
			break
		} else if pp.highWatermark == 0 {
			Logger.Printf("producer/leader/%s/%d state change to [normal]\n", pp.topic, pp.partition)
			break
		}
	}
}

func (pp *partitionProducer) updateLeader() error {
	return pp.breaker.Run(func() (err error) {
		if err = pp.parent.client.RefreshMetadata(pp.topic); err != nil {
			return err
		}

		if pp.leader, err = pp.parent.client.Leader(pp.topic, pp.partition); err != nil {
			return err
		}

		pp.output = pp.parent.getBrokerProducer(pp.leader)
		return nil
	})
}

// one per broker, constructs both an aggregator and a flusher
func (p *asyncProducer) newBrokerProducer(broker *Broker) chan<- *ProducerMessage {
	var (
		input     = make(chan *ProducerMessage)
		bridge    = make(chan *produceSet)
		responses = make(chan *ProduceResponse)
		errors    = make(chan error)
	)

	a := &brokerProducer{
		parent:         p,
		broker:         broker,
		input:          input,
		output:         bridge,
		responses:      responses,
		errors:         errors,
		buffer:         newProduceSet(p),
		currentRetries: make(map[string]map[int32]error),
	}
	go withRecover(a.run)

	f := &flusher{
		parent:    p,
		broker:    broker,
		input:     bridge,
		responses: responses,
		errors:    errors,
	}
	go withRecover(f.run)

	return input
}

// groups messages together into appropriately-sized batches for sending to the broker
type brokerProducer struct {
	parent *asyncProducer
	broker *Broker
	input  <-chan *ProducerMessage

	output    chan<- *produceSet
	responses <-chan *ProduceResponse
	errors    <-chan error

	buffer  *produceSet
	pending *produceSet
	timer   <-chan time.Time

	closing        error
	currentRetries map[string]map[int32]error
}

func (bp *brokerProducer) run() {
	var output chan<- *produceSet
	Logger.Printf("producer/broker/%d starting up\n", bp.broker.ID())

	for {
		select {
		case msg := <-bp.input:
			if msg == nil {
				goto shutdown
			}

			if reason := bp.retryReason(msg); reason != nil {
				bp.parent.retryMessages([]*ProducerMessage{msg}, reason)
				continue
			}

			if bp.buffer.wouldOverflow(msg) {
				Logger.Printf("producer/broker/%d maximum request accumulated, forcing blocking flush\n", bp.broker.ID())
				bp.flush()
				output = nil
			}

			bp.buffer.add(msg)

			if bp.buffer.readyToFlush(msg) {
				output = bp.output
			} else if bp.parent.conf.Producer.Flush.Frequency > 0 && bp.timer == nil {
				bp.timer = time.After(bp.parent.conf.Producer.Flush.Frequency)
			}
		case <-bp.timer:
			output = bp.output
		case output <- bp.buffer:
			bp.rollOver()
			output = nil
		case response := <-bp.responses:
			bp.handleResponse(response)
		case err := <-bp.errors:
			bp.handleError(err)
		}
	}

shutdown:
	if !bp.buffer.empty() {
		bp.flush()
	}
	bp.wait()
	close(bp.output)

	Logger.Printf("producer/broker/%d shut down\n", bp.broker.ID())
}

func (bp *brokerProducer) retryReason(msg *ProducerMessage) error {
	if bp.closing != nil {
		return bp.closing
	}

	if bp.currentRetries[msg.Topic] != nil {
		err := bp.currentRetries[msg.Topic][msg.Partition]
		if err != nil && msg.flags&chaser == chaser {
			// we're currently retrying this partition so we need to filter out this message
			// but now we can start processing future messages again
			Logger.Printf("producer/broker/%d state change to [normal] on %s/%d\n",
				bp.broker.ID(), msg.Topic, msg.Partition)
			delete(bp.currentRetries[msg.Topic], msg.Partition)
		}
		return err
	}
	return nil
}

func (bp *brokerProducer) wait() {
	if bp.pending != nil {
		select {
		case response := <-bp.responses:
			bp.handleResponse(response)
		case err := <-bp.errors:
			bp.handleError(err)
		}

	}
}

func (bp *brokerProducer) flush() {
	bp.wait()
	bp.output <- bp.buffer
	bp.rollOver()
}

func (bp *brokerProducer) rollOver() {
	bp.timer = nil
	bp.pending = bp.buffer
	bp.buffer = newProduceSet(bp.parent)
}

func (bp *brokerProducer) handleResponse(response *ProduceResponse) {
	// we iterate through the blocks in the request set, not the response, so that we notice
	// if the response is missing a block completely
	bp.pending.eachPartition(func(topic string, partition int32, msgs []*ProducerMessage) {
		if response == nil {
			// this only happens when RequiredAcks is NoResponse, so we have to assume success
			bp.parent.returnSuccesses(msgs)
			return
		}

		block := response.GetBlock(topic, partition)
		if block == nil {
			bp.parent.returnErrors(msgs, ErrIncompleteResponse)
			return
		}

		switch block.Err {
		// Success
		case ErrNoError:
			i := 0
			for _, msg := range msgs {
				if msg != nil {
					msg.Offset = block.Offset + int64(i)
					i++
				}
			}
			bp.parent.returnSuccesses(msgs)
		// Retriable errors
		case ErrUnknownTopicOrPartition, ErrNotLeaderForPartition, ErrLeaderNotAvailable,
			ErrRequestTimedOut, ErrNotEnoughReplicas, ErrNotEnoughReplicasAfterAppend:
			Logger.Printf("producer/broker/%d state change to [retrying] on %s/%d because %v\n",
				bp.broker.ID(), topic, partition, block.Err)
			if bp.currentRetries[topic] == nil {
				bp.currentRetries[topic] = make(map[int32]error)
			}
			bp.currentRetries[topic][partition] = block.Err
			bp.parent.retryMessages(msgs, block.Err)
			// Other non-retriable errors
		default:
			bp.parent.returnErrors(msgs, block.Err)
		}
	})
	bp.pending = nil
}

func (bp *brokerProducer) handleError(err error) {
	switch err.(type) {
	case nil:
		break
	case PacketEncodingError:
		bp.pending.eachPartition(func(topic string, partition int32, msgs []*ProducerMessage) {
			bp.parent.returnErrors(msgs, err)
		})
	default:
		Logger.Printf("producer/broker/%d state change to [closing] because %s\n", bp.broker.ID(), err)
		bp.parent.abandonBrokerConnection(bp.broker)
		_ = bp.broker.Close()
		bp.closing = err
		bp.pending.eachPartition(func(topic string, partition int32, msgs []*ProducerMessage) {
			bp.parent.retryMessages(msgs, err)
		})
	}
}

// takes a set at a time from the brokerProducer and sends to the broker
type flusher struct {
	parent    *asyncProducer
	broker    *Broker
	input     <-chan *produceSet
	responses chan<- *ProduceResponse
	errors    chan<- error
}

func (f *flusher) run() {
	for set := range f.input {
		request := set.buildRequest()
		if request == nil {
			f.errors <- nil
			continue
		}

		response, err := f.broker.Produce(request)

		if err != nil {
			f.errors <- err
		} else {
			f.responses <- response
		}
	}
}

// singleton
// effectively a "bridge" between the flushers and the dispatcher in order to avoid deadlock
// based on https://godoc.org/github.com/eapache/channels#InfiniteChannel
func (p *asyncProducer) retryHandler() {
	var msg *ProducerMessage
	buf := queue.New()

	for {
		if buf.Length() == 0 {
			msg = <-p.retries
		} else {
			select {
			case msg = <-p.retries:
			case p.input <- buf.Peek().(*ProducerMessage):
				buf.Remove()
				continue
			}
		}

		if msg == nil {
			return
		}

		buf.Add(msg)
	}
}

// produceSet

type partitionSet struct {
	msgs        []*ProducerMessage
	setToSend   *MessageSet
	bufferBytes int
}

type produceSet struct {
	msgs   map[string]map[int32]*partitionSet
	parent *asyncProducer

	bufferBytes int
	bufferCount int
}

func newProduceSet(parent *asyncProducer) *produceSet {
	return &produceSet{
		msgs:   make(map[string]map[int32]*partitionSet),
		parent: parent,
	}
}

func (ps *produceSet) add(msg *ProducerMessage) {
	partitions := ps.msgs[msg.Topic]
	if partitions == nil {
		partitions = make(map[int32]*partitionSet)
		ps.msgs[msg.Topic] = partitions
	}

	set := partitions[msg.Partition]
	if set == nil {
		set = &partitionSet{setToSend: new(MessageSet)}
		partitions[msg.Partition] = set
	}

	set.msgs = append(set.msgs, msg)
	set.bufferBytes += msg.byteSize()
	ps.bufferBytes += msg.byteSize()
	ps.bufferCount++
}

func (ps *produceSet) buildRequest() *ProduceRequest {

	req := &ProduceRequest{RequiredAcks: ps.parent.conf.Producer.RequiredAcks, Timeout: int32(ps.parent.conf.Producer.Timeout / time.Millisecond)}
	empty := true

	for topic, partitionSet := range ps.msgs {
		for partition, msgSet := range partitionSet {
			setToSend := new(MessageSet)
			setSize := 0
			for i, msg := range msgSet.msgs {
				if msg == nil {
					continue
				}
				var err error
				var key, val []byte
				if msg.Key != nil {
					if key, err = msg.Key.Encode(); err != nil {
						ps.parent.returnError(msg, err)
						msgSet.msgs[i] = nil
						continue
					}
				}

				if msg.Value != nil {
					if val, err = msg.Value.Encode(); err != nil {
						ps.parent.returnError(msg, err)
						msgSet.msgs[i] = nil
						continue
					}
				}
				if ps.parent.conf.Producer.Compression != CompressionNone && setSize+msg.byteSize() > ps.parent.conf.Producer.MaxMessageBytes {
					// compression causes message-sets to be wrapped as single messages, which have tighter
					// size requirements, so we have to respect those limits
					valBytes, err := encode(setToSend)
					if err != nil {
						Logger.Println(err) // if this happens, it's basically our fault.
						panic(err)
					}
					req.AddMessage(topic, partition, &Message{Codec: ps.parent.conf.Producer.Compression, Key: nil, Value: valBytes})
					setToSend = new(MessageSet)
					setSize = 0
				}
				setSize += msg.byteSize()

				setToSend.addMessage(&Message{Codec: CompressionNone, Key: key, Value: val})
				empty = false
			}

			if ps.parent.conf.Producer.Compression == CompressionNone {
				req.AddSet(topic, partition, setToSend)
			} else {
				valBytes, err := encode(setToSend)
				if err != nil {
					Logger.Println(err) // if this happens, it's basically our fault.
					panic(err)
				}
				req.AddMessage(topic, partition, &Message{Codec: ps.parent.conf.Producer.Compression, Key: nil, Value: valBytes})
			}
		}
	}

	if empty {
		return nil
	}
	return req
}

func (ps *produceSet) eachPartition(cb func(topic string, partition int32, msgs []*ProducerMessage)) {
	for topic, partitionSet := range ps.msgs {
		for partition, set := range partitionSet {
			cb(topic, partition, set.msgs)
		}
	}
}

func (ps *produceSet) wouldOverflow(msg *ProducerMessage) bool {
	switch {
	// Would we overflow our maximum possible size-on-the-wire? 10KiB is arbitrary overhead for safety.
	case ps.bufferBytes+msg.byteSize() >= int(MaxRequestSize-(10*1024)):
		return true
	// Would we overflow the size-limit of a compressed message-batch?
	case ps.parent.conf.Producer.Compression != CompressionNone && ps.bufferBytes+msg.byteSize() >= ps.parent.conf.Producer.MaxMessageBytes:
		return true
	// Would we overflow simply in number of messages?
	case ps.parent.conf.Producer.Flush.MaxMessages > 0 && ps.bufferCount >= ps.parent.conf.Producer.Flush.MaxMessages:
		return true
	default:
		return false
	}
}

func (ps *produceSet) readyToFlush(msg *ProducerMessage) bool {
	switch {
	// If all three config values are 0, we always flush as-fast-as-possible
	case ps.parent.conf.Producer.Flush.Frequency == 0 && ps.parent.conf.Producer.Flush.Bytes == 0 && ps.parent.conf.Producer.Flush.Messages == 0:
		return true
	// If the messages is a chaser we must flush to maintain the state-machine
	case msg.flags&chaser == chaser:
		return true
	// If we've passed the message trigger-point
	case ps.parent.conf.Producer.Flush.Messages > 0 && ps.bufferCount >= ps.parent.conf.Producer.Flush.Messages:
		return true
	// If we've passed the byte trigger-point
	case ps.parent.conf.Producer.Flush.Bytes > 0 && ps.bufferBytes >= ps.parent.conf.Producer.Flush.Bytes:
		return true
	default:
		return false
	}
}

func (ps *produceSet) empty() bool {
	return ps.bufferCount == 0
}

// utility functions

func (p *asyncProducer) shutdown() {
	Logger.Println("Producer shutting down.")
	p.inFlight.Add(1)
	p.input <- &ProducerMessage{flags: shutdown}

	p.inFlight.Wait()

	if p.ownClient {
		err := p.client.Close()
		if err != nil {
			Logger.Println("producer/shutdown failed to close the embedded client:", err)
		}
	}

	close(p.input)
	close(p.retries)
	close(p.errors)
	close(p.successes)
}

func (p *asyncProducer) returnError(msg *ProducerMessage, err error) {
	msg.clear()
	pErr := &ProducerError{Msg: msg, Err: err}
	if p.conf.Producer.Return.Errors {
		p.errors <- pErr
	} else {
		Logger.Println(pErr)
	}
	p.inFlight.Done()
}

func (p *asyncProducer) returnErrors(batch []*ProducerMessage, err error) {
	for _, msg := range batch {
		if msg != nil {
			p.returnError(msg, err)
		}
	}
}

func (p *asyncProducer) returnSuccesses(batch []*ProducerMessage) {
	for _, msg := range batch {
		if msg == nil {
			continue
		}
		if p.conf.Producer.Return.Successes {
			msg.clear()
			p.successes <- msg
		}
		p.inFlight.Done()
	}
}

func (p *asyncProducer) retryMessages(batch []*ProducerMessage, err error) {
	for _, msg := range batch {
		if msg == nil {
			continue
		}
		if msg.retries >= p.conf.Producer.Retry.Max {
			p.returnError(msg, err)
		} else {
			msg.retries++
			p.retries <- msg
		}
	}
}

func (p *asyncProducer) getBrokerProducer(broker *Broker) chan<- *ProducerMessage {
	p.brokerLock.Lock()
	defer p.brokerLock.Unlock()

	bp := p.brokers[broker]

	if bp == nil {
		bp = p.newBrokerProducer(broker)
		p.brokers[broker] = bp
		p.brokerRefs[bp] = 0
	}

	p.brokerRefs[bp]++

	return bp
}

func (p *asyncProducer) unrefBrokerProducer(broker *Broker, bp chan<- *ProducerMessage) {
	p.brokerLock.Lock()
	defer p.brokerLock.Unlock()

	p.brokerRefs[bp]--
	if p.brokerRefs[bp] == 0 {
		close(bp)
		delete(p.brokerRefs, bp)

		if p.brokers[broker] == bp {
			delete(p.brokers, broker)
		}
	}
}

func (p *asyncProducer) abandonBrokerConnection(broker *Broker) {
	p.brokerLock.Lock()
	defer p.brokerLock.Unlock()

	delete(p.brokers, broker)
}

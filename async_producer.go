package sarama

import (
	"fmt"
	"sync"
	"time"

	"github.com/eapache/go-resiliency/breaker"
	"github.com/eapache/queue"
)

func forceFlushThreshold() int {
	return int(MaxRequestSize - (10 * 1024)) // 10KiB is safety room for misc. overhead, we might want to calculate this more precisely?
}

// AsyncProducer publishes Kafka messages using a non-blocking API. It routes messages
// to the correct broker for the provided topic-partition, refreshing metadata as appropriate,
// and parses responses for errors. You must read from the Errors() channel or the
// producer will deadlock. You must call Close() or AsyncClose() on a producer to avoid
// leaks: it will not be garbage-collected automatically when it passes out of
// scope.
type AsyncProducer interface {

	// AsyncClose triggers a shutdown of the producer, flushing any messages it may have
	// buffered. The shutdown has completed when both the Errors and Successes channels
	// have been closed. When calling AsyncClose, you *must* continue to read from those
	// channels in order to drain the results of any messages in flight.
	AsyncClose()

	// Close shuts down the producer and flushes any messages it may have buffered.
	// You must call this function before a producer object passes out of scope, as
	// it may otherwise leak memory. You must call this before calling Close on the
	// underlying client.
	Close() error

	// Input is the input channel for the user to write messages to that they wish to send.
	Input() chan<- *ProducerMessage

	// Successes is the success output channel back to the user when AckSuccesses is enabled.
	// If Return.Successes is true, you MUST read from this channel or the Producer will deadlock.
	// It is suggested that you send and read messages together in a single select statement.
	Successes() <-chan *ProducerMessage

	// Errors is the error output channel back to the user. You MUST read from this channel
	// or the Producer will deadlock when the channel is full. Alternatively, you can set
	// Producer.Return.Errors in your config to false, which prevents errors to be returned.
	Errors() <-chan *ProducerError
}

type asyncProducer struct {
	client    Client
	conf      *Config
	ownClient bool

	errors                    chan *ProducerError
	input, successes, retries chan *ProducerMessage
	inFlight                  sync.WaitGroup

	brokers    map[*Broker]chan *ProducerMessage
	brokerRefs map[chan *ProducerMessage]int
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
		brokers:    make(map[*Broker]chan *ProducerMessage),
		brokerRefs: make(map[chan *ProducerMessage]int),
	}

	// launch our singleton dispatchers
	go withRecover(p.topicDispatcher)
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
	Topic string  // The Kafka topic for this message.
	Key   Encoder // The partitioning key for this message. It must implement the Encoder interface. Pre-existing Encoders include StringEncoder and ByteEncoder.
	Value Encoder // The actual message to store in Kafka. It must implement the Encoder interface. Pre-existing Encoders include StringEncoder and ByteEncoder.

	// These are filled in by the producer as the message is processed
	Offset    int64 // Offset is the offset of the message stored on the broker. This is only guaranteed to be defined if the message was successfully delivered and RequiredAcks is not NoResponse.
	Partition int32 // Partition is the partition that the message was sent to. This is only guaranteed to be defined if the message was successfully delivered.

	Metadata interface{} // This field is used to hold arbitrary data you wish to include so it will be available when receiving on the Successes and Errors channels.  Sarama completely ignores this field and is only to be used for pass-through data.

	retries int
	flags   flagSet
}

func (m *ProducerMessage) byteSize() int {
	size := 26 // the metadata overhead of CRC, flags, etc.
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

///////////////////////////////////////////
// In normal processing, a message flows through the following functions from top to bottom,
// starting at topicDispatcher (which reads from Producer.input) and ending in flusher
// (which sends the message to the broker). In cases where a message must be retried, it goes
// through retryHandler before being returned to the top of the flow.
///////////////////////////////////////////

// singleton
// dispatches messages by topic
func (p *asyncProducer) topicDispatcher() {
	handlers := make(map[string]chan *ProducerMessage)
	shuttingDown := false

	for msg := range p.input {
		if msg == nil {
			Logger.Println("Something tried to send a nil message, it was ignored.")
			continue
		}

		if msg.flags&shutdown != 0 {
			shuttingDown = true
			continue
		} else if msg.retries == 0 {
			p.inFlight.Add(1)
			if shuttingDown {
				p.returnError(msg, ErrShuttingDown)
				continue
			}
		}

		if (p.conf.Producer.Compression == CompressionNone && msg.Value != nil && msg.Value.Length() > p.conf.Producer.MaxMessageBytes) ||
			(msg.byteSize() > p.conf.Producer.MaxMessageBytes) {

			p.returnError(msg, ErrMessageSizeTooLarge)
			continue
		}

		handler := handlers[msg.Topic]
		if handler == nil {
			newHandler := make(chan *ProducerMessage, p.conf.ChannelBufferSize)
			topic := msg.Topic // block local because go's closure semantics suck
			go withRecover(func() { p.partitionDispatcher(topic, newHandler) })
			handler = newHandler
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
func (p *asyncProducer) partitionDispatcher(topic string, input chan *ProducerMessage) {
	handlers := make(map[int32]chan *ProducerMessage)
	partitioner := p.conf.Producer.Partitioner(topic)
	breaker := breaker.New(3, 1, 10*time.Second)

	for msg := range input {
		if msg.retries == 0 {
			err := breaker.Run(func() error {
				return p.assignPartition(partitioner, msg)
			})
			if err != nil {
				p.returnError(msg, err)
				continue
			}
		}

		handler := handlers[msg.Partition]
		if handler == nil {
			newHandler := make(chan *ProducerMessage, p.conf.ChannelBufferSize)
			topic := msg.Topic         // block local because go's closure semantics suck
			partition := msg.Partition // block local because go's closure semantics suck
			go withRecover(func() { p.leaderDispatcher(topic, partition, newHandler) })
			handler = newHandler
			handlers[msg.Partition] = handler
		}

		handler <- msg
	}

	for _, handler := range handlers {
		close(handler)
	}
}

// one per partition per topic
// dispatches messages to the appropriate broker
// also responsible for maintaining message order during retries
func (p *asyncProducer) leaderDispatcher(topic string, partition int32, input chan *ProducerMessage) {
	var leader *Broker
	var output chan *ProducerMessage

	breaker := breaker.New(3, 1, 10*time.Second)
	doUpdate := func() (err error) {
		if err = p.client.RefreshMetadata(topic); err != nil {
			return err
		}

		if leader, err = p.client.Leader(topic, partition); err != nil {
			return err
		}

		output = p.getBrokerProducer(leader)
		return nil
	}

	// try to prefetch the leader; if this doesn't work, we'll do a proper breaker-protected refresh-and-fetch
	// on the first message
	leader, _ = p.client.Leader(topic, partition)
	if leader != nil {
		output = p.getBrokerProducer(leader)
	}

	// highWatermark tracks the "current" retry level, which is the only one where we actually let messages through,
	// all other messages get buffered in retryState[msg.retries].buf to preserve ordering
	// retryState[msg.retries].expectChaser simply tracks whether we've seen a chaser message for a given level (and
	// therefore whether our buffer is complete and safe to flush)
	highWatermark := 0
	retryState := make([]struct {
		buf          []*ProducerMessage
		expectChaser bool
	}, p.conf.Producer.Retry.Max+1)

	for msg := range input {
		if msg.retries > highWatermark {
			// new, higher, retry level; send off a chaser so that we know when everything "in between" has made it
			// back to us and we can safely flush the backlog (otherwise we risk re-ordering messages)
			highWatermark = msg.retries
			Logger.Printf("producer/leader/%s/%d state change to [retrying-%d]\n", topic, partition, highWatermark)
			retryState[msg.retries].expectChaser = true
			p.inFlight.Add(1) // we're generating a chaser message; track it so we don't shut down while it's still inflight
			output <- &ProducerMessage{Topic: topic, Partition: partition, flags: chaser, retries: msg.retries - 1}
			Logger.Printf("producer/leader/%s/%d abandoning broker %d\n", topic, partition, leader.ID())
			p.unrefBrokerProducer(leader, output)
			output = nil
			time.Sleep(p.conf.Producer.Retry.Backoff)
		} else if highWatermark > 0 {
			// we are retrying something (else highWatermark would be 0) but this message is not a *new* retry level
			if msg.retries < highWatermark {
				// in fact this message is not even the current retry level, so buffer it for now (unless it's a just a chaser)
				if msg.flags&chaser == chaser {
					retryState[msg.retries].expectChaser = false
					p.inFlight.Done() // this chaser is now handled and will be garbage collected
				} else {
					retryState[msg.retries].buf = append(retryState[msg.retries].buf, msg)
				}
				continue
			} else if msg.flags&chaser == chaser {
				// this message is of the current retry level (msg.retries == highWatermark) and the chaser flag is set,
				// meaning this retry level is done and we can go down (at least) one level and flush that
				retryState[highWatermark].expectChaser = false
				Logger.Printf("producer/leader/%s/%d state change to [flushing-%d]\n", topic, partition, highWatermark)
				for {
					highWatermark--

					if output == nil {
						if err := breaker.Run(doUpdate); err != nil {
							p.returnErrors(retryState[highWatermark].buf, err)
							goto flushDone
						}
						Logger.Printf("producer/leader/%s/%d selected broker %d\n", topic, partition, leader.ID())
					}

					for _, msg := range retryState[highWatermark].buf {
						output <- msg
					}

				flushDone:
					retryState[highWatermark].buf = nil
					if retryState[highWatermark].expectChaser {
						Logger.Printf("producer/leader/%s/%d state change to [retrying-%d]\n", topic, partition, highWatermark)
						break
					} else {
						if highWatermark == 0 {
							Logger.Printf("producer/leader/%s/%d state change to [normal]\n", topic, partition)
							break
						}
					}

				}
				p.inFlight.Done() // this chaser is now handled and will be garbage collected
				continue
			}
		}

		// if we made it this far then the current msg contains real data, and can be sent to the next goroutine
		// without breaking any of our ordering guarantees

		if output == nil {
			if err := breaker.Run(doUpdate); err != nil {
				p.returnError(msg, err)
				time.Sleep(p.conf.Producer.Retry.Backoff)
				continue
			}
			Logger.Printf("producer/leader/%s/%d selected broker %d\n", topic, partition, leader.ID())
		}

		output <- msg
	}

	if output != nil {
		p.unrefBrokerProducer(leader, output)
	}
}

// one per broker
// groups messages together into appropriately-sized batches for sending to the broker
// based on https://godoc.org/github.com/eapache/channels#BatchingChannel
func (p *asyncProducer) messageAggregator(broker *Broker, input chan *ProducerMessage) {
	var (
		timer            <-chan time.Time
		buffer           []*ProducerMessage
		flushTriggered   chan []*ProducerMessage
		bytesAccumulated int
		defaultFlush     bool
	)

	if p.conf.Producer.Flush.Frequency == 0 && p.conf.Producer.Flush.Bytes == 0 && p.conf.Producer.Flush.Messages == 0 {
		defaultFlush = true
	}

	output := make(chan []*ProducerMessage)
	go withRecover(func() { p.flusher(broker, output) })

	for {
		select {
		case msg := <-input:
			if msg == nil {
				goto shutdown
			}

			if (bytesAccumulated+msg.byteSize() >= forceFlushThreshold()) ||
				(p.conf.Producer.Compression != CompressionNone && bytesAccumulated+msg.byteSize() >= p.conf.Producer.MaxMessageBytes) ||
				(p.conf.Producer.Flush.MaxMessages > 0 && len(buffer) >= p.conf.Producer.Flush.MaxMessages) {
				Logger.Printf("producer/aggregator/%d maximum request accumulated, forcing blocking flush\n", broker.ID())
				output <- buffer
				timer = nil
				buffer = nil
				flushTriggered = nil
				bytesAccumulated = 0
			}

			buffer = append(buffer, msg)
			bytesAccumulated += msg.byteSize()

			if defaultFlush ||
				msg.flags&chaser == chaser ||
				(p.conf.Producer.Flush.Messages > 0 && len(buffer) >= p.conf.Producer.Flush.Messages) ||
				(p.conf.Producer.Flush.Bytes > 0 && bytesAccumulated >= p.conf.Producer.Flush.Bytes) {
				flushTriggered = output
			} else if p.conf.Producer.Flush.Frequency > 0 && timer == nil {
				timer = time.After(p.conf.Producer.Flush.Frequency)
			}
		case <-timer:
			flushTriggered = output
		case flushTriggered <- buffer:
			timer = nil
			buffer = nil
			flushTriggered = nil
			bytesAccumulated = 0
		}
	}

shutdown:
	if len(buffer) > 0 {
		output <- buffer
	}
	close(output)
}

// one per broker
// takes a batch at a time from the messageAggregator and sends to the broker
func (p *asyncProducer) flusher(broker *Broker, input chan []*ProducerMessage) {
	var closing error
	currentRetries := make(map[string]map[int32]error)
	Logger.Printf("producer/flusher/%d starting up\n", broker.ID())

	for batch := range input {
		if closing != nil {
			p.retryMessages(batch, closing)
			continue
		}

		// group messages by topic/partition
		msgSets := make(map[string]map[int32][]*ProducerMessage)
		for i, msg := range batch {
			if currentRetries[msg.Topic] != nil && currentRetries[msg.Topic][msg.Partition] != nil {
				if msg.flags&chaser == chaser {
					// we can start processing this topic/partition again
					Logger.Printf("producer/flusher/%d state change to [normal] on %s/%d\n",
						broker.ID(), msg.Topic, msg.Partition)
					currentRetries[msg.Topic][msg.Partition] = nil
				}
				p.retryMessages([]*ProducerMessage{msg}, currentRetries[msg.Topic][msg.Partition])
				batch[i] = nil // to prevent it being returned/retried twice
				continue
			}

			partitionSet := msgSets[msg.Topic]
			if partitionSet == nil {
				partitionSet = make(map[int32][]*ProducerMessage)
				msgSets[msg.Topic] = partitionSet
			}

			partitionSet[msg.Partition] = append(partitionSet[msg.Partition], msg)
		}

		request := p.buildRequest(msgSets)
		if request == nil {
			continue
		}

		response, err := broker.Produce(request)

		switch err.(type) {
		case nil:
			break
		case PacketEncodingError:
			p.returnErrors(batch, err)
			continue
		default:
			Logger.Printf("producer/flusher/%d state change to [closing] because %s\n", broker.ID(), err)
			p.abandonBrokerConnection(broker)
			p.retryMessages(batch, err)
			_ = broker.Close()
			closing = err
			continue
		}

		if response == nil {
			// this only happens when RequiredAcks is NoResponse, so we have to assume success
			p.returnSuccesses(batch)
			continue
		}

		// we iterate through the blocks in the request, not the response, so that we notice
		// if the response is missing a block completely
		for topic, partitionSet := range msgSets {
			for partition, msgs := range partitionSet {

				block := response.GetBlock(topic, partition)
				if block == nil {
					p.returnErrors(msgs, ErrIncompleteResponse)
					continue
				}

				switch block.Err {
				case ErrNoError:
					// All the messages for this topic-partition were delivered successfully!
					for i := range msgs {
						msgs[i].Offset = block.Offset + int64(i)
					}
					p.returnSuccesses(msgs)
				case ErrUnknownTopicOrPartition, ErrNotLeaderForPartition, ErrLeaderNotAvailable,
					ErrRequestTimedOut, ErrNotEnoughReplicas, ErrNotEnoughReplicasAfterAppend:
					Logger.Printf("producer/flusher/%d state change to [retrying] on %s/%d because %v\n",
						broker.ID(), topic, partition, block.Err)
					if currentRetries[topic] == nil {
						currentRetries[topic] = make(map[int32]error)
					}
					currentRetries[topic][partition] = block.Err
					p.retryMessages(msgs, block.Err)
				default:
					p.returnErrors(msgs, block.Err)
				}
			}
		}
	}
	Logger.Printf("producer/flusher/%d shut down\n", broker.ID())
}

// singleton
// effectively a "bridge" between the flushers and the topicDispatcher in order to avoid deadlock
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

///////////////////////////////////////////
///////////////////////////////////////////

// utility functions

func (p *asyncProducer) shutdown() {
	Logger.Println("Producer shutting down.")
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

func (p *asyncProducer) assignPartition(partitioner Partitioner, msg *ProducerMessage) error {
	var partitions []int32
	var err error

	if partitioner.RequiresConsistency() {
		partitions, err = p.client.Partitions(msg.Topic)
	} else {
		partitions, err = p.client.WritablePartitions(msg.Topic)
	}

	if err != nil {
		return err
	}

	numPartitions := int32(len(partitions))

	if numPartitions == 0 {
		return ErrLeaderNotAvailable
	}

	choice, err := partitioner.Partition(msg, numPartitions)

	if err != nil {
		return err
	} else if choice < 0 || choice >= numPartitions {
		return ErrInvalidPartition
	}

	msg.Partition = partitions[choice]

	return nil
}

func (p *asyncProducer) buildRequest(batch map[string]map[int32][]*ProducerMessage) *ProduceRequest {

	req := &ProduceRequest{RequiredAcks: p.conf.Producer.RequiredAcks, Timeout: int32(p.conf.Producer.Timeout / time.Millisecond)}
	empty := true

	for topic, partitionSet := range batch {
		for partition, msgSet := range partitionSet {
			setToSend := new(MessageSet)
			setSize := 0
			for _, msg := range msgSet {
				var keyBytes, valBytes []byte
				var err error
				if msg.Key != nil {
					if keyBytes, err = msg.Key.Encode(); err != nil {
						p.returnError(msg, err)
						continue
					}
				}
				if msg.Value != nil {
					if valBytes, err = msg.Value.Encode(); err != nil {
						p.returnError(msg, err)
						continue
					}
				}

				if p.conf.Producer.Compression != CompressionNone && setSize+msg.byteSize() > p.conf.Producer.MaxMessageBytes {
					// compression causes message-sets to be wrapped as single messages, which have tighter
					// size requirements, so we have to respect those limits
					valBytes, err := encode(setToSend)
					if err != nil {
						Logger.Println(err) // if this happens, it's basically our fault.
						panic(err)
					}
					req.AddMessage(topic, partition, &Message{Codec: p.conf.Producer.Compression, Key: nil, Value: valBytes})
					setToSend = new(MessageSet)
					setSize = 0
				}
				setSize += msg.byteSize()

				setToSend.addMessage(&Message{Codec: CompressionNone, Key: keyBytes, Value: valBytes})
				empty = false
			}

			if p.conf.Producer.Compression == CompressionNone {
				req.AddSet(topic, partition, setToSend)
			} else {
				valBytes, err := encode(setToSend)
				if err != nil {
					Logger.Println(err) // if this happens, it's basically our fault.
					panic(err)
				}
				req.AddMessage(topic, partition, &Message{Codec: p.conf.Producer.Compression, Key: nil, Value: valBytes})
			}
		}
	}

	if empty {
		return nil
	}
	return req
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

func (p *asyncProducer) getBrokerProducer(broker *Broker) chan *ProducerMessage {
	p.brokerLock.Lock()
	defer p.brokerLock.Unlock()

	bp := p.brokers[broker]

	if bp == nil {
		bp = make(chan *ProducerMessage)
		p.brokers[broker] = bp
		p.brokerRefs[bp] = 0
		go withRecover(func() { p.messageAggregator(broker, bp) })
	}

	p.brokerRefs[bp]++

	return bp
}

func (p *asyncProducer) unrefBrokerProducer(broker *Broker, bp chan *ProducerMessage) {
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

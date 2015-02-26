package sarama

import (
	"fmt"
	"sync"
	"time"

	"github.com/eapache/go-resiliency/breaker"
)

func forceFlushThreshold() int {
	return int(MaxRequestSize - (10 * 1024)) // 10KiB is safety room for misc. overhead, we might want to calculate this more precisely?
}

// Producer publishes Kafka messages. It routes messages to the correct broker
// for the provided topic-partition, refreshing metadata as appropriate, and
// parses responses for errors. You must read from the Errors() channel or the
// producer will deadlock. You must call Close() or AsyncClose() on a producer to avoid
// leaks: it will not be garbage-collected automatically when it passes out of
// scope (this is in addition to calling Close on the underlying client, which
// is still necessary).
type Producer struct {
	client    *Client
	conf      *Config
	ownClient bool

	errors                    chan *ProducerError
	input, successes, retries chan *ProducerMessage

	brokers    map[*Broker]*brokerProducer
	brokerLock sync.Mutex
}

// NewProducer creates a new Producer using the given broker addresses and configuration.
func NewProducer(addrs []string, conf *Config) (*Producer, error) {
	client, err := NewClient(addrs, conf)
	if err != nil {
		return nil, err
	}

	p, err := NewProducerFromClient(client)
	if err != nil {
		return nil, err
	}
	p.ownClient = true
	return p, nil
}

// NewProducerFromClient creates a new Producer using the given client.
func NewProducerFromClient(client *Client) (*Producer, error) {
	// Check that we are not dealing with a closed Client before processing any other arguments
	if client.Closed() {
		return nil, ErrClosedClient
	}

	p := &Producer{
		client:    client,
		conf:      client.conf,
		errors:    make(chan *ProducerError),
		input:     make(chan *ProducerMessage),
		successes: make(chan *ProducerMessage),
		retries:   make(chan *ProducerMessage),
		brokers:   make(map[*Broker]*brokerProducer),
	}

	// launch our singleton dispatchers
	go withRecover(p.topicDispatcher)
	go withRecover(p.retryHandler)

	return p, nil
}

type flagSet int8

const (
	chaser   flagSet = 1 << iota // message is last in a group that failed
	ref                          // add a reference to a singleton channel
	unref                        // remove a reference from a singleton channel
	shutdown                     // start the shutdown process
)

// ProducerMessage is the collection of elements passed to the Producer in order to send a message.
type ProducerMessage struct {
	Topic    string      // The Kafka topic for this message.
	Key      Encoder     // The partitioning key for this message. It must implement the Encoder interface. Pre-existing Encoders include StringEncoder and ByteEncoder.
	Value    Encoder     // The actual message to store in Kafka. It must implement the Encoder interface. Pre-existing Encoders include StringEncoder and ByteEncoder.
	Metadata interface{} // This field is used to hold arbitrary data you wish to include so it will be available when receiving on the Successes and Errors channels.  Sarama completely ignores this field and is only to be used for pass-through data.

	// these are filled in by the producer as the message is processed
	offset    int64
	partition int32
	retries   int
	flags     flagSet
}

// Offset is the offset of the message stored on the broker. This is only guaranteed to be defined if
// the message was successfully delivered and RequiredAcks is not NoResponse.
func (m *ProducerMessage) Offset() int64 {
	return m.offset
}

// Partition is the partition that the message was sent to. This is only guaranteed to be defined if
// the message was successfully delivered.
func (m *ProducerMessage) Partition() int32 {
	return m.partition
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

// Errors is the error output channel back to the user. You MUST read from this channel or the Producer will deadlock.
// It is suggested that you send messages and read errors together in a single select statement.
func (p *Producer) Errors() <-chan *ProducerError {
	return p.errors
}

// Successes is the success output channel back to the user when AckSuccesses is confured.
// If AckSuccesses is true, you MUST read from this channel or the Producer will deadlock.
// It is suggested that you send and read messages together in a single select statement.
func (p *Producer) Successes() <-chan *ProducerMessage {
	return p.successes
}

// Input is the input channel for the user to write messages to that they wish to send.
func (p *Producer) Input() chan<- *ProducerMessage {
	return p.input
}

// Close shuts down the producer and flushes any messages it may have buffered.
// You must call this function before a producer object passes out of scope, as
// it may otherwise leak memory. You must call this before calling Close on the
// underlying client.
func (p *Producer) Close() error {
	p.AsyncClose()

	if p.conf.Producer.AckSuccesses {
		go withRecover(func() {
			for _ = range p.successes {
			}
		})
	}

	var errors ProducerErrors
	for event := range p.errors {
		errors = append(errors, event)
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

// AsyncClose triggers a shutdown of the producer, flushing any messages it may have
// buffered. The shutdown has completed when both the Errors and Successes channels
// have been closed. When calling AsyncClose, you *must* continue to read from those
// channels in order to drain the results of any messages in flight.
func (p *Producer) AsyncClose() {
	go withRecover(func() {
		p.input <- &ProducerMessage{flags: shutdown}
	})
}

///////////////////////////////////////////
// In normal processing, a message flows through the following functions from top to bottom,
// starting at topicDispatcher (which reads from Producer.input) and ending in flusher
// (which sends the message to the broker). In cases where a message must be retried, it goes
// through retryHandler before being returned to the top of the flow.
///////////////////////////////////////////

// singleton
// dispatches messages by topic
func (p *Producer) topicDispatcher() {
	handlers := make(map[string]chan *ProducerMessage)

	for msg := range p.input {
		if msg == nil {
			Logger.Println("Something tried to send a nil message, it was ignored.")
			continue
		}

		if msg.flags&shutdown != 0 {
			Logger.Println("Producer shutting down.")
			break
		}

		if (p.conf.Producer.Compression == CompressionNone && msg.Value != nil && msg.Value.Length() > p.conf.Producer.MaxMessageBytes) ||
			(msg.byteSize() > p.conf.Producer.MaxMessageBytes) {

			p.returnError(msg, ErrMessageSizeTooLarge)
			continue
		}

		handler := handlers[msg.Topic]
		if handler == nil {
			p.retries <- &ProducerMessage{flags: ref}
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

	p.retries <- &ProducerMessage{flags: shutdown}

	for msg := range p.input {
		p.returnError(msg, ErrShuttingDown)
	}

	if p.ownClient {
		err := p.client.Close()
		if err != nil {
			p.errors <- &ProducerError{Err: err}
		}
	}
	close(p.errors)
	close(p.successes)
}

// one per topic
// partitions messages, then dispatches them by partition
func (p *Producer) partitionDispatcher(topic string, input chan *ProducerMessage) {
	handlers := make(map[int32]chan *ProducerMessage)
	partitioner := p.conf.Producer.Partitioner()

	for msg := range input {
		if msg.retries == 0 {
			err := p.assignPartition(partitioner, msg)
			if err != nil {
				p.returnError(msg, err)
				continue
			}
		}

		handler := handlers[msg.partition]
		if handler == nil {
			p.retries <- &ProducerMessage{flags: ref}
			newHandler := make(chan *ProducerMessage, p.conf.ChannelBufferSize)
			topic := msg.Topic         // block local because go's closure semantics suck
			partition := msg.partition // block local because go's closure semantics suck
			go withRecover(func() { p.leaderDispatcher(topic, partition, newHandler) })
			handler = newHandler
			handlers[msg.partition] = handler
		}

		handler <- msg
	}

	for _, handler := range handlers {
		close(handler)
	}
	p.retries <- &ProducerMessage{flags: unref}
}

// one per partition per topic
// dispatches messages to the appropriate broker
// also responsible for maintaining message order during retries
func (p *Producer) leaderDispatcher(topic string, partition int32, input chan *ProducerMessage) {
	var leader *Broker
	var output chan *ProducerMessage

	breaker := breaker.New(3, 1, 10*time.Second)
	doUpdate := func() (err error) {
		if err = p.client.RefreshTopicMetadata(topic); err != nil {
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
			Logger.Printf("producer/leader state change to [retrying-%d] on %s/%d\n", highWatermark, topic, partition)
			retryState[msg.retries].expectChaser = true
			output <- &ProducerMessage{Topic: topic, partition: partition, flags: chaser, retries: msg.retries - 1}
			Logger.Printf("producer/leader abandoning broker %d on %s/%d\n", leader.ID(), topic, partition)
			p.unrefBrokerProducer(leader)
			output = nil
			time.Sleep(p.conf.Producer.Retry.Backoff)
		} else if highWatermark > 0 {
			// we are retrying something (else highWatermark would be 0) but this message is not a *new* retry level
			if msg.retries < highWatermark {
				// in fact this message is not even the current retry level, so buffer it for now (unless it's a just a chaser)
				if msg.flags&chaser == chaser {
					retryState[msg.retries].expectChaser = false
				} else {
					retryState[msg.retries].buf = append(retryState[msg.retries].buf, msg)
				}
				continue
			} else if msg.flags&chaser == chaser {
				// this message is of the current retry level (msg.retries == highWatermark) and the chaser flag is set,
				// meaning this retry level is done and we can go down (at least) one level and flush that
				retryState[highWatermark].expectChaser = false
				Logger.Printf("producer/leader state change to [normal-%d] on %s/%d\n", highWatermark, topic, partition)
				for {
					highWatermark--
					Logger.Printf("producer/leader state change to [flushing-%d] on %s/%d\n", highWatermark, topic, partition)

					if output == nil {
						if err := breaker.Run(doUpdate); err != nil {
							p.returnErrors(retryState[highWatermark].buf, err)
							goto flushDone
						}
						Logger.Printf("producer/leader selected broker %d on %s/%d\n", leader.ID(), topic, partition)
					}

					for _, msg := range retryState[highWatermark].buf {
						output <- msg
					}

				flushDone:
					retryState[highWatermark].buf = nil
					if retryState[highWatermark].expectChaser {
						Logger.Printf("producer/leader state change to [retrying-%d] on %s/%d\n", highWatermark, topic, partition)
						break
					} else {
						Logger.Printf("producer/leader state change to [normal-%d] on %s/%d\n", highWatermark, topic, partition)
						if highWatermark == 0 {
							break
						}
					}

				}
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
			Logger.Printf("producer/leader selected broker %d on %s/%d\n", leader.ID(), topic, partition)
		}

		output <- msg
	}

	p.unrefBrokerProducer(leader)
	p.retries <- &ProducerMessage{flags: unref}
}

// one per broker
// groups messages together into appropriately-sized batches for sending to the broker
// based on https://godoc.org/github.com/eapache/channels#BatchingChannel
func (p *Producer) messageAggregator(broker *Broker, input chan *ProducerMessage) {
	var ticker *time.Ticker
	var timer <-chan time.Time
	if p.conf.Producer.Flush.Frequency > 0 {
		ticker = time.NewTicker(p.conf.Producer.Flush.Frequency)
		timer = ticker.C
	}

	var buffer []*ProducerMessage
	var doFlush chan []*ProducerMessage
	var bytesAccumulated int

	flusher := make(chan []*ProducerMessage)
	go withRecover(func() { p.flusher(broker, flusher) })

	for {
		select {
		case msg := <-input:
			if msg == nil {
				goto shutdown
			}

			if (bytesAccumulated+msg.byteSize() >= forceFlushThreshold()) ||
				(p.conf.Producer.Compression != CompressionNone && bytesAccumulated+msg.byteSize() >= p.conf.Producer.MaxMessageBytes) ||
				(p.conf.Producer.Flush.MaxMessages > 0 && len(buffer) >= p.conf.Producer.Flush.MaxMessages) {
				Logger.Println("producer/aggregator maximum request accumulated, forcing blocking flush")
				flusher <- buffer
				buffer = nil
				doFlush = nil
				bytesAccumulated = 0
			}

			buffer = append(buffer, msg)
			bytesAccumulated += msg.byteSize()

			if len(buffer) >= p.conf.Producer.Flush.Messages ||
				(p.conf.Producer.Flush.Bytes > 0 && bytesAccumulated >= p.conf.Producer.Flush.Bytes) {
				doFlush = flusher
			}
		case <-timer:
			doFlush = flusher
		case doFlush <- buffer:
			buffer = nil
			doFlush = nil
			bytesAccumulated = 0
		}
	}

shutdown:
	if ticker != nil {
		ticker.Stop()
	}
	if len(buffer) > 0 {
		flusher <- buffer
	}
	close(flusher)
}

// one per broker
// takes a batch at a time from the messageAggregator and sends to the broker
func (p *Producer) flusher(broker *Broker, input chan []*ProducerMessage) {
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
			if currentRetries[msg.Topic] != nil && currentRetries[msg.Topic][msg.partition] != nil {
				if msg.flags&chaser == chaser {
					// we can start processing this topic/partition again
					Logger.Printf("producer/flusher/%d state change to [normal] on %s/%d\n",
						broker.ID(), msg.Topic, msg.partition)
					currentRetries[msg.Topic][msg.partition] = nil
				}
				p.retryMessages([]*ProducerMessage{msg}, currentRetries[msg.Topic][msg.partition])
				batch[i] = nil // to prevent it being returned/retried twice
				continue
			}

			partitionSet := msgSets[msg.Topic]
			if partitionSet == nil {
				partitionSet = make(map[int32][]*ProducerMessage)
				msgSets[msg.Topic] = partitionSet
			}

			partitionSet[msg.partition] = append(partitionSet[msg.partition], msg)
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
			p.client.disconnectBroker(broker)
			Logger.Printf("producer/flusher/%d state change to [closing] because %s\n", broker.ID(), err)
			closing = err
			p.retryMessages(batch, err)
			continue
		}

		if response == nil {
			// this only happens when RequiredAcks is NoResponse, so we have to assume success
			if p.conf.Producer.AckSuccesses {
				p.returnSuccesses(batch)
			}
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
					if p.conf.Producer.AckSuccesses {
						for i := range msgs {
							msgs[i].offset = block.Offset + int64(i)
						}
						p.returnSuccesses(msgs)
					}
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
	p.retries <- &ProducerMessage{flags: unref}
}

// singleton
// effectively a "bridge" between the flushers and the topicDispatcher in order to avoid deadlock
// based on https://godoc.org/github.com/eapache/channels#InfiniteChannel
func (p *Producer) retryHandler() {
	var buf []*ProducerMessage
	var msg *ProducerMessage
	refs := 0
	shuttingDown := false

	for {
		if len(buf) == 0 {
			msg = <-p.retries
		} else {
			select {
			case msg = <-p.retries:
			case p.input <- buf[0]:
				buf = buf[1:]
				continue
			}
		}

		if msg.flags&ref != 0 {
			refs++
		} else if msg.flags&unref != 0 {
			refs--
			if refs == 0 && shuttingDown {
				break
			}
		} else if msg.flags&shutdown != 0 {
			shuttingDown = true
			if refs == 0 {
				break
			}
		} else {
			buf = append(buf, msg)
		}
	}

	close(p.retries)
	for i := range buf {
		p.input <- buf[i]
	}
	close(p.input)
}

///////////////////////////////////////////
///////////////////////////////////////////

// utility functions

func (p *Producer) assignPartition(partitioner Partitioner, msg *ProducerMessage) error {
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

	choice, err := partitioner.Partition(msg.Key, numPartitions)

	if err != nil {
		return err
	} else if choice < 0 || choice >= numPartitions {
		return ErrInvalidPartition
	}

	msg.partition = partitions[choice]

	return nil
}

func (p *Producer) buildRequest(batch map[string]map[int32][]*ProducerMessage) *ProduceRequest {

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

func (p *Producer) returnError(msg *ProducerMessage, err error) {
	msg.flags = 0
	msg.retries = 0
	p.errors <- &ProducerError{Msg: msg, Err: err}
}

func (p *Producer) returnErrors(batch []*ProducerMessage, err error) {
	for _, msg := range batch {
		if msg != nil {
			p.returnError(msg, err)
		}
	}
}

func (p *Producer) returnSuccesses(batch []*ProducerMessage) {
	for _, msg := range batch {
		if msg != nil {
			msg.flags = 0
			p.successes <- msg
		}
	}
}

func (p *Producer) retryMessages(batch []*ProducerMessage, err error) {
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

type brokerProducer struct {
	input chan *ProducerMessage
	refs  int
}

func (p *Producer) getBrokerProducer(broker *Broker) chan *ProducerMessage {
	p.brokerLock.Lock()
	defer p.brokerLock.Unlock()

	producer := p.brokers[broker]

	if producer == nil {
		p.retries <- &ProducerMessage{flags: ref}
		producer = &brokerProducer{
			refs:  1,
			input: make(chan *ProducerMessage),
		}
		p.brokers[broker] = producer
		go withRecover(func() { p.messageAggregator(broker, producer.input) })
	} else {
		producer.refs++
	}

	return producer.input
}

func (p *Producer) unrefBrokerProducer(broker *Broker) {
	p.brokerLock.Lock()
	defer p.brokerLock.Unlock()

	producer := p.brokers[broker]

	if producer != nil {
		producer.refs--
		if producer.refs == 0 {
			close(producer.input)
			delete(p.brokers, broker)
		}
	}
}

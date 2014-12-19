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

// ProducerConfig is used to pass multiple configuration options to NewProducer.
type ProducerConfig struct {
	Partitioner       PartitionerConstructor // Generates partitioners for choosing the partition to send messages to (defaults to hash).
	RequiredAcks      RequiredAcks           // The level of acknowledgement reliability needed from the broker (defaults to WaitForLocal).
	Timeout           time.Duration          // The maximum duration the broker will wait the receipt of the number of RequiredAcks. This is only relevant when RequiredAcks is set to WaitForAll or a number > 1. Only supports millisecond resolution, nanoseconds will be truncated.
	Compression       CompressionCodec       // The type of compression to use on messages (defaults to no compression).
	FlushMsgCount     int                    // The number of messages needed to trigger a flush. This is a minimum, not an upper limit (use MaxMessagesPerReq for that).
	FlushFrequency    time.Duration          // If this amount of time elapses without a flush, one will be queued. This is a minimum, not an upper limit.
	FlushByteCount    int                    // If this many bytes of messages are accumulated, a flush will be triggered. This is a minimum, not an upper limit.
	AckSuccesses      bool                   // If enabled, successfully delivered messages will be returned on the Successes channel.
	MaxMessageBytes   int                    // The maximum permitted size of a message (defaults to 1000000)
	MaxMessagesPerReq int                    // The maximum number of messages the producer will send in a single broker request. Defaults to 0 for unlimited. The global setting MaxRequestSize still applies.
	ChannelBufferSize int                    // The size of the buffers of the channels between the different goroutines. Defaults to 0 (unbuffered).
	RetryBackoff      time.Duration          // The amount of time to wait for the cluster to elect a new leader before processing retries. Defaults to 250ms.
}

// NewProducerConfig creates a new ProducerConfig instance with sensible defaults.
func NewProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		Partitioner:     NewHashPartitioner,
		RequiredAcks:    WaitForLocal,
		MaxMessageBytes: 1000000,
		RetryBackoff:    250 * time.Millisecond,
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

	if config.FlushMsgCount < 0 {
		return ConfigurationError("Invalid FlushMsgCount")
	}

	if config.FlushByteCount < 0 {
		return ConfigurationError("Invalid FlushByteCount")
	} else if config.FlushByteCount >= forceFlushThreshold() {
		Logger.Println("ProducerConfig.FlushByteCount too close to MaxRequestSize; it will be ignored.")
	}

	if config.FlushFrequency < 0 {
		return ConfigurationError("Invalid FlushFrequency")
	}

	if config.Partitioner == nil {
		return ConfigurationError("No partitioner set")
	}

	if config.MaxMessageBytes <= 0 {
		return ConfigurationError("Invalid MaxMessageBytes")
	} else if config.MaxMessageBytes >= forceFlushThreshold() {
		Logger.Println("ProducerConfig.MaxMessageBytes too close to MaxRequestSize; it will be ignored.")
	}

	if config.MaxMessagesPerReq < 0 || (config.MaxMessagesPerReq > 0 && config.MaxMessagesPerReq < config.FlushMsgCount) {
		return ConfigurationError("Invalid MaxMessagesPerReq, must be non-negative and >= FlushMsgCount if set")
	}

	if config.RetryBackoff < 0 {
		return ConfigurationError("Invalid RetryBackoff")
	}

	return nil
}

// Producer publishes Kafka messages. It routes messages to the correct broker
// for the provided topic-partition, refreshing metadata as appropriate, and
// parses responses for errors. You must read from the Errors() channel or the
// producer will deadlock. You must call Close() on a producer to avoid
// leaks: it will not be garbage-collected automatically when it passes out of
// scope (this is in addition to calling Close on the underlying client, which
// is still necessary).
type Producer struct {
	client *Client
	config ProducerConfig

	errors                    chan *ProduceError
	input, successes, retries chan *MessageToSend

	brokers    map[*Broker]*brokerWorker
	brokerLock sync.Mutex
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

	p := &Producer{
		client:    client,
		config:    *config,
		errors:    make(chan *ProduceError),
		input:     make(chan *MessageToSend),
		successes: make(chan *MessageToSend),
		retries:   make(chan *MessageToSend),
		brokers:   make(map[*Broker]*brokerWorker),
	}

	// launch our singleton dispatchers
	go withRecover(p.topicDispatcher)
	go withRecover(p.retryHandler)

	return p, nil
}

type flagSet int8

const (
	retried  flagSet = 1 << iota // message has been retried
	chaser                       // message is last in a group that failed
	ref                          // add a reference to a singleton channel
	unref                        // remove a reference from a singleton channel
	shutdown                     // start the shutdown process
)

// MessageToSend is the collection of elements passed to the Producer in order to send a message.
type MessageToSend struct {
	Topic      string
	Key, Value Encoder

	// these are filled in by the producer as the message is processed
	offset    int64
	partition int32
	flags     flagSet
}

// Offset is the offset of the message stored on the broker. This is only guaranteed to be defined if
// the message was successfully delivered and RequiredAcks is not NoResponse.
func (m *MessageToSend) Offset() int64 {
	return m.offset
}

// Partition is the partition that the message was sent to. This is only guaranteed to be defined if
// the message was successfully delivered.
func (m *MessageToSend) Partition() int32 {
	return m.partition
}

func (m *MessageToSend) byteSize() int {
	size := 26 // the metadata overhead of CRC, flags, etc.
	if m.Key != nil {
		size += m.Key.Length()
	}
	if m.Value != nil {
		size += m.Value.Length()
	}
	return size
}

// ProduceError is the type of error generated when the producer fails to deliver a message.
// It contains the original MessageToSend as well as the actual error value.
type ProduceError struct {
	Msg *MessageToSend
	Err error
}

// ProduceErrors is a type that wraps a batch of "ProduceError"s and implements the Error interface.
// It can be returned from the Producer's Close method to avoid the need to manually drain the Errors channel
// when closing a producer.
type ProduceErrors []*ProduceError

func (pe ProduceErrors) Error() string {
	return fmt.Sprintf("kafka: Failed to deliver %d messages.", len(pe))
}

// Errors is the error output channel back to the user. You MUST read from this channel or the Producer will deadlock.
// It is suggested that you send messages and read errors together in a single select statement.
func (p *Producer) Errors() <-chan *ProduceError {
	return p.errors
}

// Successes is the success output channel back to the user when AckSuccesses is configured.
// If AckSuccesses is true, you MUST read from this channel or the Producer will deadlock.
// It is suggested that you send and read messages together in a single select statement.
func (p *Producer) Successes() <-chan *MessageToSend {
	return p.successes
}

// Input is the input channel for the user to write messages to that they wish to send.
func (p *Producer) Input() chan<- *MessageToSend {
	return p.input
}

// Close shuts down the producer and flushes any messages it may have buffered.
// You must call this function before a producer object passes out of scope, as
// it may otherwise leak memory. You must call this before calling Close on the
// underlying client.
func (p *Producer) Close() error {
	go withRecover(func() {
		p.input <- &MessageToSend{flags: shutdown}
	})

	if p.config.AckSuccesses {
		go withRecover(func() {
			for _ = range p.successes {
			}
		})
	}

	var errors ProduceErrors
	for event := range p.errors {
		errors = append(errors, event)
	}

	close(p.successes)

	if len(errors) > 0 {
		return errors
	}
	return nil
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
	handlers := make(map[string]chan *MessageToSend)

	for msg := range p.input {
		if msg == nil {
			Logger.Println("Something tried to send a nil message, it was ignored.")
			continue
		}

		if msg.flags&shutdown != 0 {
			Logger.Println("Producer shutting down.")
			break
		}

		if (p.config.Compression == CompressionNone && msg.Value != nil && msg.Value.Length() > p.config.MaxMessageBytes) ||
			(msg.byteSize() > p.config.MaxMessageBytes) {

			p.returnError(msg, MessageSizeTooLarge)
			continue
		}

		handler := handlers[msg.Topic]
		if handler == nil {
			p.retries <- &MessageToSend{flags: ref}
			newHandler := make(chan *MessageToSend, p.config.ChannelBufferSize)
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

	p.retries <- &MessageToSend{flags: shutdown}

	for msg := range p.input {
		p.returnError(msg, ShuttingDown)
	}

	close(p.errors)
}

// one per topic
// partitions messages, then dispatches them by partition
func (p *Producer) partitionDispatcher(topic string, input chan *MessageToSend) {
	handlers := make(map[int32]chan *MessageToSend)
	partitioner := p.config.Partitioner()

	for msg := range input {
		if msg.flags&retried == 0 {
			err := p.assignPartition(partitioner, msg)
			if err != nil {
				p.returnError(msg, err)
				continue
			}
		}

		handler := handlers[msg.partition]
		if handler == nil {
			p.retries <- &MessageToSend{flags: ref}
			newHandler := make(chan *MessageToSend, p.config.ChannelBufferSize)
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
	p.retries <- &MessageToSend{flags: unref}
}

// one per partition per topic
// dispatches messages to the appropriate broker
// also responsible for maintaining message order during retries
func (p *Producer) leaderDispatcher(topic string, partition int32, input chan *MessageToSend) {
	var leader *Broker
	var output chan *MessageToSend
	var backlog []*MessageToSend
	breaker := breaker.New(3, 1, 10*time.Second)
	doUpdate := func() (err error) {
		if err = p.client.RefreshTopicMetadata(topic); err != nil {
			return err
		}

		if leader, err = p.client.Leader(topic, partition); err != nil {
			return err
		}

		output = p.getBrokerWorker(leader)
		return nil
	}

	// try to prefetch the leader; if this doesn't work, we'll do a proper breaker-protected refresh-and-fetch
	// on the first message
	leader, _ = p.client.Leader(topic, partition)
	if leader != nil {
		output = p.getBrokerWorker(leader)
	}

	for msg := range input {
		if msg.flags&retried == 0 {
			// normal case
			if backlog != nil {
				backlog = append(backlog, msg)
				continue
			}
		} else if msg.flags&chaser == 0 {
			// retry flag set, chaser flag not set
			if backlog == nil {
				// on the very first retried message we send off a chaser so that we know when everything "in between" has made it
				// back to us and we can safely flush the backlog (otherwise we risk re-ordering messages)
				Logger.Printf("producer/leader state change to [retrying] on %s/%d\n", topic, partition)
				output <- &MessageToSend{Topic: topic, partition: partition, flags: chaser}
				backlog = make([]*MessageToSend, 0)
				p.unrefBrokerWorker(leader)
				output = nil
				time.Sleep(p.config.RetryBackoff)
			}
		} else {
			// retry *and* chaser flag set, flush the backlog and return to normal processing
			Logger.Printf("producer/leader state change to [flushing] on %s/%d\n", topic, partition)
			if output == nil {
				if err := breaker.Run(doUpdate); err != nil {
					p.returnErrors(backlog, err)
					backlog = nil
					continue
				}
			}

			for _, msg := range backlog {
				output <- msg
			}
			Logger.Printf("producer/leader state change to [normal] on %s/%d\n", topic, partition)

			backlog = nil
			continue
		}

		if output == nil {
			if err := breaker.Run(doUpdate); err != nil {
				p.returnError(msg, err)
				continue
			}
		}

		output <- msg
	}

	p.unrefBrokerWorker(leader)
	p.retries <- &MessageToSend{flags: unref}
}

// one per broker
// groups messages together into appropriately-sized batches for sending to the broker
// based on https://godoc.org/github.com/eapache/channels#BatchingChannel
func (p *Producer) messageAggregator(broker *Broker, input chan *MessageToSend) {
	var ticker *time.Ticker
	var timer <-chan time.Time
	if p.config.FlushFrequency > 0 {
		ticker = time.NewTicker(p.config.FlushFrequency)
		timer = ticker.C
	}

	var buffer []*MessageToSend
	var doFlush chan []*MessageToSend
	var bytesAccumulated int

	flusher := make(chan []*MessageToSend)
	go withRecover(func() { p.flusher(broker, flusher) })

	for {
		select {
		case msg := <-input:
			if msg == nil {
				goto shutdown
			}

			if (bytesAccumulated+msg.byteSize() >= forceFlushThreshold()) ||
				(p.config.Compression != CompressionNone && bytesAccumulated+msg.byteSize() >= p.config.MaxMessageBytes) ||
				(p.config.MaxMessagesPerReq > 0 && len(buffer) >= p.config.MaxMessagesPerReq) {
				Logger.Println("producer/aggregator maximum request accumulated, forcing blocking flush")
				flusher <- buffer
				buffer = nil
				doFlush = nil
				bytesAccumulated = 0
			}

			buffer = append(buffer, msg)
			bytesAccumulated += msg.byteSize()

			if len(buffer) >= p.config.FlushMsgCount ||
				(p.config.FlushByteCount > 0 && bytesAccumulated >= p.config.FlushByteCount) {
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
func (p *Producer) flusher(broker *Broker, input chan []*MessageToSend) {
	var closing error
	currentRetries := make(map[string]map[int32]error)

	for batch := range input {
		if closing != nil {
			p.retryMessages(batch, closing)
			continue
		}

		// group messages by topic/partition
		msgSets := make(map[string]map[int32][]*MessageToSend)
		for i, msg := range batch {
			if currentRetries[msg.Topic] != nil && currentRetries[msg.Topic][msg.partition] != nil {
				if msg.flags&chaser == chaser {
					// we can start processing this topic/partition again
					Logger.Printf("producer/flusher state change to [normal] on %s/%d\n",
						msg.Topic, msg.partition)
					currentRetries[msg.Topic][msg.partition] = nil
				}
				p.retryMessages([]*MessageToSend{msg}, currentRetries[msg.Topic][msg.partition])
				batch[i] = nil // to prevent it being returned/retried twice
				continue
			}

			partitionSet := msgSets[msg.Topic]
			if partitionSet == nil {
				partitionSet = make(map[int32][]*MessageToSend)
				msgSets[msg.Topic] = partitionSet
			}

			partitionSet[msg.partition] = append(partitionSet[msg.partition], msg)
		}

		request := p.buildRequest(msgSets)
		if request == nil {
			continue
		}

		response, err := broker.Produce(p.client.id, request)

		switch err {
		case nil:
			break
		case EncodingError:
			p.returnErrors(batch, err)
			continue
		default:
			p.client.disconnectBroker(broker)
			Logger.Println("producer/flusher state change to [closing] because", err)
			closing = err
			p.retryMessages(batch, err)
			continue
		}

		if response == nil {
			// this only happens when RequiredAcks is NoResponse, so we have to assume success
			if p.config.AckSuccesses {
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
					p.returnErrors(msgs, IncompleteResponse)
					continue
				}

				switch block.Err {
				case NoError:
					// All the messages for this topic-partition were delivered successfully!
					if p.config.AckSuccesses {
						for i := range msgs {
							msgs[i].offset = block.Offset + int64(i)
						}
						p.returnSuccesses(msgs)
					}
				case UnknownTopicOrPartition, NotLeaderForPartition, LeaderNotAvailable:
					Logger.Printf("producer/flusher state change to [retrying] on %s/%d because %v\n",
						topic, partition, block.Err)
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
	p.retries <- &MessageToSend{flags: unref}
}

// singleton
// effectively a "bridge" between the flushers and the topicDispatcher in order to avoid deadlock
// based on https://godoc.org/github.com/eapache/channels#InfiniteChannel
func (p *Producer) retryHandler() {
	var buf []*MessageToSend
	var msg *MessageToSend
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

func (p *Producer) assignPartition(partitioner Partitioner, msg *MessageToSend) error {
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
		return LeaderNotAvailable
	}

	choice, err := partitioner.Partition(msg.Key, numPartitions)

	if err != nil {
		return err
	} else if choice < 0 || choice >= numPartitions {
		return InvalidPartition
	}

	msg.partition = partitions[choice]

	return nil
}

func (p *Producer) buildRequest(batch map[string]map[int32][]*MessageToSend) *ProduceRequest {

	req := &ProduceRequest{RequiredAcks: p.config.RequiredAcks, Timeout: int32(p.config.Timeout / time.Millisecond)}
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

				if p.config.Compression != CompressionNone && setSize+msg.byteSize() > p.config.MaxMessageBytes {
					// compression causes message-sets to be wrapped as single messages, which have tighter
					// size requirements, so we have to respect those limits
					valBytes, err := encode(setToSend)
					if err != nil {
						Logger.Println(err) // if this happens, it's basically our fault.
						panic(err)
					}
					req.AddMessage(topic, partition, &Message{Codec: p.config.Compression, Key: nil, Value: valBytes})
					setToSend = new(MessageSet)
					setSize = 0
				}
				setSize += msg.byteSize()

				setToSend.addMessage(&Message{Codec: CompressionNone, Key: keyBytes, Value: valBytes})
				empty = false
			}

			if p.config.Compression == CompressionNone {
				req.AddSet(topic, partition, setToSend)
			} else {
				valBytes, err := encode(setToSend)
				if err != nil {
					Logger.Println(err) // if this happens, it's basically our fault.
					panic(err)
				}
				req.AddMessage(topic, partition, &Message{Codec: p.config.Compression, Key: nil, Value: valBytes})
			}
		}
	}

	if empty {
		return nil
	}
	return req
}

func (p *Producer) returnError(msg *MessageToSend, err error) {
	msg.flags = 0
	p.errors <- &ProduceError{Msg: msg, Err: err}
}

func (p *Producer) returnErrors(batch []*MessageToSend, err error) {
	for _, msg := range batch {
		if msg != nil {
			p.returnError(msg, err)
		}
	}
}

func (p *Producer) returnSuccesses(batch []*MessageToSend) {
	for _, msg := range batch {
		if msg != nil {
			msg.flags = 0
			p.successes <- msg
		}
	}
}

func (p *Producer) retryMessages(batch []*MessageToSend, err error) {
	for _, msg := range batch {
		if msg == nil {
			continue
		}
		if msg.flags&retried == retried {
			p.returnError(msg, err)
		} else {
			msg.flags |= retried
			p.retries <- msg
		}
	}
}

type brokerWorker struct {
	input chan *MessageToSend
	refs  int
}

func (p *Producer) getBrokerWorker(broker *Broker) chan *MessageToSend {
	p.brokerLock.Lock()
	defer p.brokerLock.Unlock()

	worker := p.brokers[broker]

	if worker == nil {
		p.retries <- &MessageToSend{flags: ref}
		worker = &brokerWorker{
			refs:  1,
			input: make(chan *MessageToSend),
		}
		p.brokers[broker] = worker
		go withRecover(func() { p.messageAggregator(broker, worker.input) })
	} else {
		worker.refs++
	}

	return worker.input
}

func (p *Producer) unrefBrokerWorker(broker *Broker) {
	p.brokerLock.Lock()
	defer p.brokerLock.Unlock()

	worker := p.brokers[broker]

	if worker != nil {
		worker.refs--
		if worker.refs == 0 {
			close(worker.input)
			delete(p.brokers, broker)
		}
	}
}

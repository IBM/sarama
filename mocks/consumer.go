package mocks

import (
	"sync"
	"sync/atomic"

	"github.com/IBM/sarama"
)

// Consumer implements sarama's Consumer interface for testing purposes.
// Before you can start consuming from this consumer, you have to register
// topic/partitions using ExpectConsumePartition, and set expectations on them.
type Consumer struct {
	l                  sync.Mutex
	t                  ErrorReporter
	config             *sarama.Config
	partitionConsumers map[string]map[int32]*PartitionConsumer
	metadata           map[string][]int32
}

// NewConsumer returns a new mock Consumer instance. The t argument should
// be the *testing.T instance of your test method. An error will be written to it if
// an expectation is violated. The config argument can be set to nil; if it is
// non-nil it is validated.
func NewConsumer(t ErrorReporter, config *sarama.Config) *Consumer {
	if config == nil {
		config = sarama.NewConfig()
	}
	if err := config.Validate(); err != nil {
		t.Errorf("Invalid mock configuration provided: %s", err.Error())
	}

	c := &Consumer{
		t:                  t,
		config:             config,
		partitionConsumers: make(map[string]map[int32]*PartitionConsumer),
	}
	return c
}

///////////////////////////////////////////////////
// Consumer interface implementation
///////////////////////////////////////////////////

// ConsumePartition implements the ConsumePartition method from the sarama.Consumer interface.
// Before you can start consuming a partition, you have to set expectations on it using
// ExpectConsumePartition. You can only consume a partition once per consumer.
func (c *Consumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.partitionConsumers[topic] == nil || c.partitionConsumers[topic][partition] == nil {
		c.t.Errorf("No expectations set for %s/%d", topic, partition)
		return nil, errOutOfExpectations
	}

	pc := c.partitionConsumers[topic][partition]
	if pc.consumed {
		return nil, sarama.ConfigurationError("The topic/partition is already being consumed")
	}

	if pc.offset != AnyOffset && pc.offset != offset {
		c.t.Errorf("Unexpected offset when calling ConsumePartition for %s/%d. Expected %d, got %d.", topic, partition, pc.offset, offset)
	}

	pc.consumed = true
	return pc, nil
}

// Topics returns a list of topics, as registered with SetTopicMetadata
func (c *Consumer) Topics() ([]string, error) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.metadata == nil {
		c.t.Errorf("Unexpected call to Topics. Initialize the mock's topic metadata with SetTopicMetadata.")
		return nil, sarama.ErrOutOfBrokers
	}

	var result []string
	for topic := range c.metadata {
		result = append(result, topic)
	}
	return result, nil
}

// Partitions returns the list of parititons for the given topic, as registered with SetTopicMetadata
func (c *Consumer) Partitions(topic string) ([]int32, error) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.metadata == nil {
		c.t.Errorf("Unexpected call to Partitions. Initialize the mock's topic metadata with SetTopicMetadata.")
		return nil, sarama.ErrOutOfBrokers
	}
	if c.metadata[topic] == nil {
		return nil, sarama.ErrUnknownTopicOrPartition
	}

	return c.metadata[topic], nil
}

func (c *Consumer) HighWaterMarks() map[string]map[int32]int64 {
	c.l.Lock()
	defer c.l.Unlock()

	hwms := make(map[string]map[int32]int64, len(c.partitionConsumers))
	for topic, partitionConsumers := range c.partitionConsumers {
		hwm := make(map[int32]int64, len(partitionConsumers))
		for partition, pc := range partitionConsumers {
			hwm[partition] = pc.HighWaterMarkOffset()
		}
		hwms[topic] = hwm
	}

	return hwms
}

// Close implements the Close method from the sarama.Consumer interface. It will close
// all registered PartitionConsumer instances.
func (c *Consumer) Close() error {
	c.l.Lock()
	defer c.l.Unlock()

	for _, partitions := range c.partitionConsumers {
		for _, partitionConsumer := range partitions {
			_ = partitionConsumer.Close()
		}
	}

	return nil
}

// Pause implements Consumer.
func (c *Consumer) Pause(topicPartitions map[string][]int32) {
	c.l.Lock()
	defer c.l.Unlock()

	for topic, partitions := range topicPartitions {
		for _, partition := range partitions {
			if topicConsumers, ok := c.partitionConsumers[topic]; ok {
				if partitionConsumer, ok := topicConsumers[partition]; ok {
					partitionConsumer.Pause()
				}
			}
		}
	}
}

// Resume implements Consumer.
func (c *Consumer) Resume(topicPartitions map[string][]int32) {
	c.l.Lock()
	defer c.l.Unlock()

	for topic, partitions := range topicPartitions {
		for _, partition := range partitions {
			if topicConsumers, ok := c.partitionConsumers[topic]; ok {
				if partitionConsumer, ok := topicConsumers[partition]; ok {
					partitionConsumer.Resume()
				}
			}
		}
	}
}

// PauseAll implements Consumer.
func (c *Consumer) PauseAll() {
	c.l.Lock()
	defer c.l.Unlock()

	for _, partitions := range c.partitionConsumers {
		for _, partitionConsumer := range partitions {
			partitionConsumer.Pause()
		}
	}
}

// ResumeAll implements Consumer.
func (c *Consumer) ResumeAll() {
	c.l.Lock()
	defer c.l.Unlock()

	for _, partitions := range c.partitionConsumers {
		for _, partitionConsumer := range partitions {
			partitionConsumer.Resume()
		}
	}
}

///////////////////////////////////////////////////
// Expectation API
///////////////////////////////////////////////////

// SetTopicMetadata sets the clusters topic/partition metadata,
// which will be returned by Topics() and Partitions().
func (c *Consumer) SetTopicMetadata(metadata map[string][]int32) {
	c.l.Lock()
	defer c.l.Unlock()

	c.metadata = metadata
}

// ExpectConsumePartition will register a topic/partition, so you can set expectations on it.
// The registered PartitionConsumer will be returned, so you can set expectations
// on it using method chaining. Once a topic/partition is registered, you are
// expected to start consuming it using ConsumePartition. If that doesn't happen,
// an error will be written to the error reporter once the mock consumer is closed. It also expects
// that the message and error channels be written with YieldMessage and YieldError accordingly,
// and be fully consumed once the mock consumer is closed if ExpectMessagesDrainedOnClose or
// ExpectErrorsDrainedOnClose have been called.
func (c *Consumer) ExpectConsumePartition(topic string, partition int32, offset int64) *PartitionConsumer {
	c.l.Lock()
	defer c.l.Unlock()

	if c.partitionConsumers[topic] == nil {
		c.partitionConsumers[topic] = make(map[int32]*PartitionConsumer)
	}

	if c.partitionConsumers[topic][partition] == nil {
		highWatermarkOffset := offset
		if offset == sarama.OffsetOldest {
			highWatermarkOffset = 0
		}

		c.partitionConsumers[topic][partition] = &PartitionConsumer{
			highWaterMarkOffset: highWatermarkOffset,
			t:                   c.t,
			topic:               topic,
			partition:           partition,
			offset:              offset,
			messages:            make(chan *sarama.ConsumerMessage, c.config.ChannelBufferSize),
			suppressedMessages:  make(chan *sarama.ConsumerMessage, c.config.ChannelBufferSize),
			errors:              make(chan *sarama.ConsumerError, c.config.ChannelBufferSize),
		}
	}

	return c.partitionConsumers[topic][partition]
}

///////////////////////////////////////////////////
// PartitionConsumer mock type
///////////////////////////////////////////////////

// PartitionConsumer implements sarama's PartitionConsumer interface for testing purposes.
// It is returned by the mock Consumers ConsumePartitionMethod, but only if it is
// registered first using the Consumer's ExpectConsumePartition method. Before consuming the
// Errors and Messages channel, you should specify what values will be provided on these
// channels using YieldMessage and YieldError.
type PartitionConsumer struct {
	highWaterMarkOffset           int64 // must be at the top of the struct because https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	suppressedHighWaterMarkOffset int64
	l                             sync.Mutex
	t                             ErrorReporter
	topic                         string
	partition                     int32
	offset                        int64
	messages                      chan *sarama.ConsumerMessage
	suppressedMessages            chan *sarama.ConsumerMessage
	errors                        chan *sarama.ConsumerError
	singleClose                   sync.Once
	consumed                      bool
	errorsShouldBeDrained         bool
	messagesShouldBeDrained       bool
	paused                        bool
}

///////////////////////////////////////////////////
// PartitionConsumer interface implementation
///////////////////////////////////////////////////

// AsyncClose implements the AsyncClose method from the sarama.PartitionConsumer interface.
func (pc *PartitionConsumer) AsyncClose() {
	pc.singleClose.Do(func() {
		close(pc.suppressedMessages)
		close(pc.messages)
		close(pc.errors)
	})
}

// Close implements the Close method from the sarama.PartitionConsumer interface. It will
// verify whether the partition consumer was actually started.
func (pc *PartitionConsumer) Close() error {
	if !pc.consumed {
		pc.t.Errorf("Expectations set on %s/%d, but no partition consumer was started.", pc.topic, pc.partition)
		return errPartitionConsumerNotStarted
	}

	if pc.errorsShouldBeDrained && len(pc.errors) > 0 {
		pc.t.Errorf("Expected the errors channel for %s/%d to be drained on close, but found %d errors.", pc.topic, pc.partition, len(pc.errors))
	}

	if pc.messagesShouldBeDrained && len(pc.messages) > 0 {
		pc.t.Errorf("Expected the messages channel for %s/%d to be drained on close, but found %d messages.", pc.topic, pc.partition, len(pc.messages))
	}

	pc.AsyncClose()

	var (
		closeErr error
		wg       sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()

		errs := make(sarama.ConsumerErrors, 0)
		for err := range pc.errors {
			errs = append(errs, err)
		}

		if len(errs) > 0 {
			closeErr = errs
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range pc.messages {
			// drain
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range pc.suppressedMessages {
			// drain
		}
	}()

	wg.Wait()
	return closeErr
}

// Errors implements the Errors method from the sarama.PartitionConsumer interface.
func (pc *PartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return pc.errors
}

// Messages implements the Messages method from the sarama.PartitionConsumer interface.
func (pc *PartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return pc.messages
}

func (pc *PartitionConsumer) HighWaterMarkOffset() int64 {
	return atomic.LoadInt64(&pc.highWaterMarkOffset)
}

// Pause implements the Pause method from the sarama.PartitionConsumer interface.
func (pc *PartitionConsumer) Pause() {
	pc.l.Lock()
	defer pc.l.Unlock()

	pc.suppressedHighWaterMarkOffset = atomic.LoadInt64(&pc.highWaterMarkOffset)

	pc.paused = true
}

// Resume implements the Resume method from the sarama.PartitionConsumer interface.
func (pc *PartitionConsumer) Resume() {
	pc.l.Lock()
	defer pc.l.Unlock()

	pc.highWaterMarkOffset = atomic.LoadInt64(&pc.suppressedHighWaterMarkOffset)
	for len(pc.suppressedMessages) > 0 {
		msg := <-pc.suppressedMessages
		pc.messages <- msg
	}

	pc.paused = false
}

// IsPaused implements the IsPaused method from the sarama.PartitionConsumer interface.
func (pc *PartitionConsumer) IsPaused() bool {
	pc.l.Lock()
	defer pc.l.Unlock()

	return pc.paused
}

///////////////////////////////////////////////////
// Expectation API
///////////////////////////////////////////////////

// YieldMessage will yield a messages Messages channel of this partition consumer
// when it is consumed. By default, the mock consumer will not verify whether this
// message was consumed from the Messages channel, because there are legitimate
// reasons forthis not to happen. ou can call ExpectMessagesDrainedOnClose so it will
// verify that the channel is empty on close.
func (pc *PartitionConsumer) YieldMessage(msg *sarama.ConsumerMessage) *PartitionConsumer {
	pc.l.Lock()
	defer pc.l.Unlock()

	msg.Topic = pc.topic
	msg.Partition = pc.partition

	if pc.paused {
		msg.Offset = atomic.AddInt64(&pc.suppressedHighWaterMarkOffset, 1) - 1
		pc.suppressedMessages <- msg
	} else {
		msg.Offset = atomic.AddInt64(&pc.highWaterMarkOffset, 1) - 1
		pc.messages <- msg
	}

	return pc
}

// YieldError will yield an error on the Errors channel of this partition consumer
// when it is consumed. By default, the mock consumer will not verify whether this error was
// consumed from the Errors channel, because there are legitimate reasons for this
// not to happen. You can call ExpectErrorsDrainedOnClose so it will verify that
// the channel is empty on close.
func (pc *PartitionConsumer) YieldError(err error) *PartitionConsumer {
	pc.errors <- &sarama.ConsumerError{
		Topic:     pc.topic,
		Partition: pc.partition,
		Err:       err,
	}

	return pc
}

// ExpectMessagesDrainedOnClose sets an expectation on the partition consumer
// that the messages channel will be fully drained when Close is called. If this
// expectation is not met, an error is reported to the error reporter.
func (pc *PartitionConsumer) ExpectMessagesDrainedOnClose() *PartitionConsumer {
	pc.messagesShouldBeDrained = true

	return pc
}

// ExpectErrorsDrainedOnClose sets an expectation on the partition consumer
// that the errors channel will be fully drained when Close is called. If this
// expectation is not met, an error is reported to the error reporter.
func (pc *PartitionConsumer) ExpectErrorsDrainedOnClose() *PartitionConsumer {
	pc.errorsShouldBeDrained = true

	return pc
}

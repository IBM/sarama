package mocks

import (
	"sync"

	"github.com/Shopify/sarama"
)

type Consumer struct {
	l                  sync.Mutex
	t                  ErrorReporter
	config             *sarama.Config
	partitionConsumers map[string]map[int32]*PartitionConsumer
}

type PartitionConsumer struct {
	l            sync.Mutex
	t            ErrorReporter
	topic        string
	partition    int32
	expectations chan *consumerExpectation
	messages     chan *sarama.ConsumerMessage
	errors       chan *sarama.ConsumerError
	consumed     bool
}

func NewConsumer(t ErrorReporter, config *sarama.Config) *Consumer {
	if config == nil {
		config = sarama.NewConfig()
	}

	c := &Consumer{
		t:                  t,
		config:             config,
		partitionConsumers: make(map[string]map[int32]*PartitionConsumer),
	}
	return c
}

func (c *Consumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.partitionConsumers[topic] == nil || c.partitionConsumers[topic][partition] == nil {
		c.t.Errorf("No expectations set for %s/%d", topic, partition)
		return nil, errOutOfExpectations
	}

	pc := c.partitionConsumers[topic][partition]
	pc.consumed = true
	go pc.handleExpectations()
	return pc, nil
}

// Close implements the Close method from the sarama.Consumer interface.
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

func (c *Consumer) OnPartition(topic string, partition int32) *PartitionConsumer {
	c.l.Lock()
	defer c.l.Unlock()

	if c.partitionConsumers[topic] == nil {
		c.partitionConsumers[topic] = make(map[int32]*PartitionConsumer)
	}

	if c.partitionConsumers[topic][partition] == nil {
		c.partitionConsumers[topic][partition] = &PartitionConsumer{
			t:            c.t,
			topic:        topic,
			partition:    partition,
			expectations: make(chan *consumerExpectation, 1000),
			messages:     make(chan *sarama.ConsumerMessage, c.config.ChannelBufferSize),
			errors:       make(chan *sarama.ConsumerError, c.config.ChannelBufferSize),
		}
	}

	return c.partitionConsumers[topic][partition]
}

// AsyncClose implements the AsyncClose method from the sarama.PartitionConsumer interface.
func (pc *PartitionConsumer) AsyncClose() {
	close(pc.expectations)
}

// Close implements the Close method from the sarama.PartitionConsumer interface.
func (pc *PartitionConsumer) Close() error {
	if !pc.consumed {
		pc.t.Errorf("Expectations set on %s/%d, but no partition consumer was started.", pc.topic, pc.partition)
		return errPartitionConsumerNotStarted
	}

	pc.AsyncClose()

	var (
		closeErr error
		wg       sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()

		var errs = make(sarama.ConsumerErrors, 0)
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
		for _ = range pc.messages {
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

func (pc *PartitionConsumer) handleExpectations() {
	pc.l.Lock()
	defer pc.l.Unlock()

	var offset int64
	for ex := range pc.expectations {
		if ex.Err != nil {
			pc.errors <- &sarama.ConsumerError{
				Topic:     pc.topic,
				Partition: pc.partition,
				Err:       ex.Err,
			}
		} else {
			offset++

			ex.Msg.Topic = pc.topic
			ex.Msg.Partition = pc.partition
			ex.Msg.Offset = offset

			pc.messages <- ex.Msg
		}
	}

	close(pc.messages)
	close(pc.errors)
}

func (pc *PartitionConsumer) ExpectMessage(msg *sarama.ConsumerMessage) {
	pc.expectations <- &consumerExpectation{Msg: msg}
}

func (pc *PartitionConsumer) ExpectError(err error) {
	pc.expectations <- &consumerExpectation{Err: err}
}

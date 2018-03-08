package sarama

import (
	"fmt"
)

// TopicConsumer consumes a single topic starting from a given offset.
type TopicConsumer interface {
	// Messages returns the read channel for the messages that are returned by
	// the broker.
	Messages() <-chan *ConsumerMessage

	Errors() <-chan error

	Close() error
}

type consumedPartition struct {
	forwarder         *forwarder
	partitionConsumer PartitionConsumer
}

type topicConsumer struct {
	topic              string
	client             Client
	master             Consumer
	consumedPartitions []consumedPartition
	messages           chan *ConsumerMessage
	errors             chan error
}

func NewTopicConsumer(client Client, topic string, offsets map[int32]int64) (TopicConsumer, error) {
	partitions, err := client.Partitions(topic)

	if err != nil {
		return nil, err
	}

	master, err := NewConsumerFromClient(client)

	if err != nil {
		return nil, err
	}

	consumer := &topicConsumer{
		topic:    topic,
		client:   client,
		master:   master,
		messages: make(chan *ConsumerMessage, client.Config().ChannelBufferSize),
		errors:   make(chan error),
	}

	for _, partition := range partitions {
		consumer.initPartition(partition, offsets[partition])
	}

	return consumer, nil
}

func (sc *topicConsumer) initPartition(partition int32, offset int64) error {
	oldestOffset, err := sc.client.GetOffset(sc.topic, partition, OffsetOldest)

	if err != nil {
		return err
	}

	newestOffset, err := sc.client.GetOffset(sc.topic, partition, OffsetNewest)

	if err != nil {
		return err
	}

	resumeFrom := offset

	if oldestOffset > resumeFrom || newestOffset < resumeFrom {
		return fmt.Errorf("offset for %v/%v is out of range of available offsets (%v..%v)", sc.topic, partition, newestOffset, oldestOffset)
	}

	partitionConsumer, err := sc.master.ConsumePartition(sc.topic, partition, resumeFrom)

	if err != nil {
		return err
	}

	forwarder := newForwarder(partitionConsumer.Messages(), partitionConsumer.Errors())

	go forwarder.forwardTo(sc.messages, sc.errors)

	sc.consumedPartitions = append(sc.consumedPartitions, consumedPartition{
		forwarder:         forwarder,
		partitionConsumer: partitionConsumer,
	})

	return nil
}

func (sc *topicConsumer) Messages() <-chan *ConsumerMessage {
	return sc.messages
}

func (sc *topicConsumer) Errors() <-chan error {
	return sc.errors
}

func (sc *topicConsumer) Close() error {
	for _, consumedPartition := range sc.consumedPartitions {
		consumedPartition.forwarder.Close()
		consumedPartition.partitionConsumer.Close()
	}

	if err := sc.master.Close(); err != nil {
		return err
	}

	close(sc.messages)

	return nil
}

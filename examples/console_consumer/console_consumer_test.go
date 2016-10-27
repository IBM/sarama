package main

import (
	"errors"
	"testing"

	"github.com/Shopify/sarama"
)

func TestConsumerOK(t *testing.T) {
	defaultConsumerConstructor = newFakeConsumer

	consumer, err := newConsumer(nil, nil)
	if err != nil {
		t.Errorf("unexpected error from newConsumer: %s\n", err)
	}

	if consumer == nil {
		t.Errorf("unexpected nil consumer")
	}
}

func TestConsumerFailed(t *testing.T) {
	err := errors.New("unable to connect to kafka")
	defaultConsumerConstructor = createFakeConsumerWithError(sarama.ErrOutOfBrokers)

	consumer, err := newConsumer(nil, nil)
	if err == nil {
		t.Errorf("expected new consumer to fail")
	}

	if consumer != nil {
		t.Errorf("expected consumer to be nil")
	}
}

func TestPartitionConsumerOK(t *testing.T) {
	defaultConsumerConstructor = createFakeConsumerWithConsumerPartition()

	consumer, err := newConsumer(nil, nil)
	if err != nil {
		t.Errorf("unexpected error from newConsumer: %s\n", err)
	}

	partitionConsumer, err := consumer.ConsumePartition("hello", 0, 20)
	if err != nil {
		t.Errorf("unexpected error calling ConsumerPartition: %s", err)
	}

	if partitionConsumer == nil {
		t.Errorf("unexpected nil partition consumer")
	}
}

func TestPartitionConsumerFailed(t *testing.T) {
	defaultConsumerConstructor = createFakeConsumerWithConsumerPartitionWithError(sarama.ErrOffsetOutOfRange)

	consumer, err := newConsumer(nil, nil)
	if err != nil {
		t.Errorf("unexpected error from newConsumer: %s\n", err)
	}

	partitionConsumer, err := consumer.ConsumePartition("hello", 0, 20)
	if err == nil {
		t.Errorf("expected new partitionConsumer to fail")
	}

	if partitionConsumer == nil {
		t.Errorf("expected partition consumer to be nil")
	}
}

func createFakeConsumerWithError(err error) consumerConstructor {
	return func(addrs []string, config *sarama.Config) (sarama.Consumer, error) {
		return nil, err
	}
}

func newFakeConsumer(addrs []string, config *sarama.Config) (sarama.Consumer, error) {
	consumer := &fakeConsumer{}
	return consumer, nil
}

func createFakeConsumerWithConsumerPartition() consumerConstructor {
	return func(addrs []string, config *sarama.Config) (sarama.Consumer, error) {
		consumer := &fakeConsumer{
			partitionConsumer: &fakePartitionConsumer{},
		}
		return consumer, nil
	}
}

func createFakeConsumerWithConsumerPartitionWithError(err error) consumerConstructor {
	return func(addrs []string, config *sarama.Config) (sarama.Consumer, error) {
		consumer := &fakeConsumer{
			partitionConsumer:    &fakePartitionConsumer{},
			errPartitionConsumer: err,
		}
		return consumer, nil
	}
}

type fakeConsumer struct {
	topics               []string
	topic                string
	partition            int32
	offset               int64
	err                  error
	partitionConsumer    sarama.PartitionConsumer
	errPartitionConsumer error
}

func (fake *fakeConsumer) Topics() ([]string, error) {
	return fake.topics, fake.err
}

func (fake *fakeConsumer) Partitions(topic string) ([]int32, error) {
	return nil, nil
}

func (fake *fakeConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	return fake.partitionConsumer, fake.errPartitionConsumer
}

func (fake *fakeConsumer) Close() error {
	return fake.err
}

type fakePartitionConsumer struct {
}

func (fake *fakePartitionConsumer) AsyncClose() {}

func (fake *fakePartitionConsumer) Close() error {
	return nil
}

func (fake *fakePartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return nil
}

func (fake *fakePartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return nil
}

func (fake *fakePartitionConsumer) HighWaterMarkOffset() int64 {
	return 0
}

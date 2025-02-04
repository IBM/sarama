/*
Package mocks provides mocks that can be used for testing applications
that use Sarama. The mock types provided by this package implement the
interfaces Sarama exports, so you can use them for dependency injection
in your tests.

All mock instances require you to set expectations on them before you
can use them. It will determine how the mock will behave. If an
expectation is not met, it will make your test fail.

NOTE: this package currently does not fall under the API stability
guarantee of Sarama as it is still considered experimental.
*/
package mocks

import (
	"errors"
	"fmt"

	"github.com/IBM/sarama"
)

// ErrorReporter is a simple interface that includes the testing.T methods we use to report
// expectation violations when using the mock objects.
type ErrorReporter interface {
	Errorf(string, ...interface{})
}

// ValueChecker is a function type to be set in each expectation of the producer mocks
// to check the value passed.
type ValueChecker func(val []byte) error

// MessageChecker is a function type to be set in each expectation of the producer mocks
// to check the message passed.
type MessageChecker func(*sarama.ProducerMessage) error

// messageValueChecker wraps a ValueChecker into a MessageChecker.
// Failure to encode the message value will return an error and not call
// the wrapped ValueChecker.
func messageValueChecker(f ValueChecker) MessageChecker {
	if f == nil {
		return nil
	}
	return func(msg *sarama.ProducerMessage) error {
		val, err := msg.Value.Encode()
		if err != nil {
			return fmt.Errorf("Input message encoding failed: %w", err)
		}
		return f(val)
	}
}

var (
	errProduceSuccess              error = nil
	errOutOfExpectations                 = errors.New("no more expectations set on mock")
	errPartitionConsumerNotStarted       = errors.New("the partition consumer was never started")
)

const AnyOffset int64 = -1000

type producerExpectation struct {
	Result        error
	CheckFunction MessageChecker
}

// TopicConfig describes a mock topic structure for the mock producers’ partitioning needs.
type TopicConfig struct {
	overridePartitions map[string]int32
	defaultPartitions  int32
}

// NewTopicConfig makes a configuration which defaults to 32 partitions for every topic.
func NewTopicConfig() *TopicConfig {
	return &TopicConfig{
		overridePartitions: make(map[string]int32, 0),
		defaultPartitions:  32,
	}
}

// SetDefaultPartitions sets the number of partitions any topic not explicitly configured otherwise
// (by SetPartitions) will have from the perspective of created partitioners.
func (pc *TopicConfig) SetDefaultPartitions(n int32) {
	pc.defaultPartitions = n
}

// SetPartitions sets the number of partitions the partitioners will see for specific topics. This
// only applies to messages produced after setting them.
func (pc *TopicConfig) SetPartitions(partitions map[string]int32) {
	for p, n := range partitions {
		pc.overridePartitions[p] = n
	}
}

func (pc *TopicConfig) partitions(topic string) int32 {
	if n, found := pc.overridePartitions[topic]; found {
		return n
	}
	return pc.defaultPartitions
}

// NewTestConfig returns a config meant to be used by tests.
// Due to inconsistencies with the request versions the clients send using the default Kafka version
// and the response versions our mocks use, we default to the minimum Kafka version in most tests
func NewTestConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Consumer.Retry.Backoff = 0
	config.Producer.Retry.Backoff = 0
	config.Version = sarama.MinVersion
	return config
}

//go:build !functional

package sarama

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/require"
)

func expectResultsWithTimeout(t *testing.T, p AsyncProducer, successCount, errorCount int, timeout time.Duration) {
	t.Helper()
	expect := successCount + errorCount
	defer func() {
		if successCount != 0 || errorCount != 0 {
			t.Error("Unexpected successes", successCount, "or errors", errorCount)
		}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for expect > 0 {
		select {
		case <-timer.C:
			return
		case msg := <-p.Errors():
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
			errorCount--
			expect--
			if errorCount < 0 {
				t.Error(msg.Err)
			}
		case msg := <-p.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
			successCount--
			expect--
			if successCount < 0 {
				t.Error("Too many successes")
			}
		}
	}
}

func expectResults(t *testing.T, p AsyncProducer, successCount, errorCount int) {
	expectResultsWithTimeout(t, p, successCount, errorCount, 5*time.Minute)
}

type testPartitioner chan *int32

func (p testPartitioner) Partition(msg *ProducerMessage, numPartitions int32) (int32, error) {
	part := <-p
	if part == nil {
		return 0, errors.New("BOOM")
	}

	return *part, nil
}

func (p testPartitioner) RequiresConsistency() bool {
	return true
}

func (p testPartitioner) feed(partition int32) {
	p <- &partition
}

type flakyEncoder bool

func (f flakyEncoder) Length() int {
	return len(TestMessage)
}

func (f flakyEncoder) Encode() ([]byte, error) {
	if !f {
		return nil, errors.New("flaky encoding error")
	}
	return []byte(TestMessage), nil
}

func TestAsyncProducer(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.Return.Successes = true
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage), Metadata: i}
	}
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
			if msg.Metadata.(int) != i {
				t.Error("Message metadata did not match")
			}
		case <-time.After(time.Second):
			t.Errorf("Timeout waiting for msg #%d", i)
			goto done
		}
	}
done:
	closeProducer(t, producer)
	leader.Close()
	seedBroker.Close()
}

func TestAsyncProducerMultipleFlushes(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)
	leader.Returns(prodSuccess)
	leader.Returns(prodSuccess)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 5
	config.Producer.Return.Successes = true
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for flush := 0; flush < 3; flush++ {
		for i := 0; i < 5; i++ {
			producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
		}
		expectResults(t, producer, 5, 0)
	}

	closeProducer(t, producer)
	leader.Close()
	seedBroker.Close()
}

func TestAsyncProducerMultipleBrokers(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader0 := NewMockBroker(t, 2)
	leader1 := NewMockBroker(t, 3)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader0.Addr(), leader0.BrokerID())
	metadataResponse.AddBroker(leader1.Addr(), leader1.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader0.BrokerID(), nil, nil, nil, ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader1.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	prodResponse0 := new(ProduceResponse)
	prodResponse0.AddTopicPartition("my_topic", 0, ErrNoError)
	leader0.Returns(prodResponse0)

	prodResponse1 := new(ProduceResponse)
	prodResponse1.AddTopicPartition("my_topic", 1, ErrNoError)
	leader1.Returns(prodResponse1)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 5
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = NewRoundRobinPartitioner
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	expectResults(t, producer, 10, 0)

	closeProducer(t, producer)
	leader1.Close()
	leader0.Close()
	seedBroker.Close()
}

func TestAsyncProducerCustomPartitioner(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	prodResponse := new(ProduceResponse)
	prodResponse.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodResponse)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 2
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = func(topic string) Partitioner {
		p := make(testPartitioner)
		go func() {
			p.feed(0)
			p <- nil
			p <- nil
			p <- nil
			p.feed(0)
		}()
		return p
	}
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	expectResults(t, producer, 2, 3)

	closeProducer(t, producer)
	leader.Close()
	seedBroker.Close()
}

func TestAsyncProducerFailureRetry(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader1 := NewMockBroker(t, 2)
	leader2 := NewMockBroker(t, 3)

	metadataLeader1 := new(MetadataResponse)
	metadataLeader1.AddBroker(leader1.Addr(), leader1.BrokerID())
	metadataLeader1.AddTopicPartition("my_topic", 0, leader1.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataLeader1)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.Return.Successes = true
	config.Producer.Retry.Backoff = 0
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	prodNotLeader := new(ProduceResponse)
	prodNotLeader.AddTopicPartition("my_topic", 0, ErrNotLeaderForPartition)
	leader1.Returns(prodNotLeader)

	metadataLeader2 := new(MetadataResponse)
	metadataLeader2.AddBroker(leader2.Addr(), leader2.BrokerID())
	metadataLeader2.AddTopicPartition("my_topic", 0, leader2.BrokerID(), nil, nil, nil, ErrNoError)
	leader1.Returns(metadataLeader2)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader2.Returns(prodSuccess)
	expectResults(t, producer, 10, 0)
	leader1.Close()

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	leader2.Returns(prodSuccess)
	expectResults(t, producer, 10, 0)

	leader2.Close()
	closeProducer(t, producer)
}

func TestAsyncProducerRecoveryWithRetriesDisabled(t *testing.T) {
	tt := func(t *testing.T, kErr KError) {
		seedBroker := NewMockBroker(t, 0)
		broker1 := NewMockBroker(t, 1)
		broker2 := NewMockBroker(t, 2)

		mockLeader := func(leaderID int32) *MockMetadataResponse {
			return NewMockMetadataResponse(t).
				SetController(seedBroker.BrokerID()).
				SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
				SetBroker(broker1.Addr(), broker1.BrokerID()).
				SetBroker(broker2.Addr(), broker2.BrokerID()).
				SetLeader("my_topic", 0, leaderID).
				SetLeader("my_topic", 1, leaderID)
		}

		seedBroker.SetHandlerByMap(
			map[string]MockResponse{
				"MetadataRequest": mockLeader(broker1.BrokerID()),
			},
		)

		config := NewTestConfig()
		config.ClientID = "TestAsyncProducerRecoveryWithRetriesDisabled"
		config.Producer.Flush.Messages = 2
		config.Producer.Flush.Frequency = 100 * time.Millisecond
		config.Producer.Return.Successes = true
		config.Producer.Retry.Max = 0 // disable!
		config.Producer.Retry.Backoff = 0
		config.Producer.Partitioner = NewManualPartitioner
		producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
		if err != nil {
			t.Fatal(err)
		}

		broker1.SetHandlerByMap(
			map[string]MockResponse{
				"MetadataRequest": mockLeader(broker1.BrokerID()),
				"ProduceRequest": NewMockProduceResponse(t).
					SetError("my_topic", 0, kErr).
					SetError("my_topic", 1, kErr),
			},
		)

		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage), Partition: 0}
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage), Partition: 1}
		expectResults(t, producer, 0, 2)

		seedBroker.SetHandlerByMap(
			map[string]MockResponse{
				"MetadataRequest": mockLeader(broker2.BrokerID()),
			},
		)
		broker1.SetHandlerByMap(
			map[string]MockResponse{
				"MetadataRequest": mockLeader(broker2.BrokerID()),
			},
		)
		broker2.SetHandlerByMap(
			map[string]MockResponse{
				"MetadataRequest": mockLeader(broker2.BrokerID()),
				"ProduceRequest": NewMockProduceResponse(t).
					SetError("my_topic", 0, ErrNoError).
					SetError("my_topic", 1, ErrNoError),
			},
		)

		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage), Partition: 0}
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage), Partition: 1}
		expectResults(t, producer, 2, 0)

		closeProducer(t, producer)
		seedBroker.Close()
		broker1.Close()
		broker2.Close()
	}

	t.Run("retriable error", func(t *testing.T) {
		tt(t, ErrNotLeaderForPartition)
	})

	t.Run("non-retriable error", func(t *testing.T) {
		tt(t, ErrNotController)
	})
}

func TestAsyncProducerEncoderFailures(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)
	leader.Returns(prodSuccess)
	leader.Returns(prodSuccess)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 1
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = NewManualPartitioner
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for flush := 0; flush < 3; flush++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: flakyEncoder(true), Value: flakyEncoder(false)}
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: flakyEncoder(false), Value: flakyEncoder(true)}
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: flakyEncoder(true), Value: flakyEncoder(true)}
		expectResults(t, producer, 1, 2)
	}

	closeProducer(t, producer)
	leader.Close()
	seedBroker.Close()
}

// If a Kafka broker becomes unavailable and then returns back in service, then
// producer reconnects to it and continues sending messages.
func TestAsyncProducerBrokerBounce(t *testing.T) {
	// Given
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)
	leaderAddr := leader.Addr()

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leaderAddr, leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 1
	config.Producer.Return.Successes = true
	config.Producer.Retry.Backoff = 0
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	leader.Returns(prodSuccess)
	expectResults(t, producer, 1, 0)

	// When: a broker connection gets reset by a broker (network glitch, restart, you name it).
	leader.Close()                               // producer should get EOF
	leader = NewMockBrokerAddr(t, 2, leaderAddr) // start it up again right away for giggles
	leader.Returns(metadataResponse)             // tell it to go to broker 2 again

	// Then: a produced message goes through the new broker connection.
	producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	leader.Returns(prodSuccess)
	expectResults(t, producer, 1, 0)

	closeProducer(t, producer)
	seedBroker.Close()
	leader.Close()
}

func TestAsyncProducerBrokerBounceWithStaleMetadata(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader1 := NewMockBroker(t, 2)
	leader2 := NewMockBroker(t, 3)

	metadataLeader1 := new(MetadataResponse)
	metadataLeader1.AddBroker(leader1.Addr(), leader1.BrokerID())
	metadataLeader1.AddTopicPartition("my_topic", 0, leader1.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataLeader1)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 3
	config.Producer.Retry.Backoff = 0
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	leader1.Close()                     // producer should get EOF
	seedBroker.Returns(metadataLeader1) // tell it to go to leader1 again even though it's still down
	seedBroker.Returns(metadataLeader1) // tell it to go to leader1 again even though it's still down

	// ok fine, tell it to go to leader2 finally
	metadataLeader2 := new(MetadataResponse)
	metadataLeader2.AddBroker(leader2.Addr(), leader2.BrokerID())
	metadataLeader2.AddTopicPartition("my_topic", 0, leader2.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataLeader2)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader2.Returns(prodSuccess)
	expectResults(t, producer, 10, 0)
	seedBroker.Close()
	leader2.Close()

	closeProducer(t, producer)
}

func TestAsyncProducerMultipleRetries(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader1 := NewMockBroker(t, 2)
	leader2 := NewMockBroker(t, 3)

	metadataLeader1 := new(MetadataResponse)
	metadataLeader1.AddBroker(leader1.Addr(), leader1.BrokerID())
	metadataLeader1.AddTopicPartition("my_topic", 0, leader1.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataLeader1)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 4
	config.Producer.Retry.Backoff = 0
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	prodNotLeader := new(ProduceResponse)
	prodNotLeader.AddTopicPartition("my_topic", 0, ErrNotLeaderForPartition)
	leader1.Returns(prodNotLeader)

	metadataLeader2 := new(MetadataResponse)
	metadataLeader2.AddBroker(leader2.Addr(), leader2.BrokerID())
	metadataLeader2.AddTopicPartition("my_topic", 0, leader2.BrokerID(), nil, nil, nil, ErrNoError)

	leader1.Returns(metadataLeader2)
	leader2.Returns(prodNotLeader)
	leader2.Returns(metadataLeader1)
	leader1.Returns(prodNotLeader)
	leader1.Returns(metadataLeader1)
	leader1.Returns(prodNotLeader)
	leader1.Returns(metadataLeader2)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader2.Returns(prodSuccess)
	expectResults(t, producer, 10, 0)

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	leader2.Returns(prodSuccess)
	expectResults(t, producer, 10, 0)

	seedBroker.Close()
	leader1.Close()
	leader2.Close()
	closeProducer(t, producer)
}

func TestAsyncProducerMultipleRetriesWithBackoffFunc(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader1 := NewMockBroker(t, 2)
	leader2 := NewMockBroker(t, 3)

	metadataLeader1 := new(MetadataResponse)
	metadataLeader1.AddBroker(leader1.Addr(), leader1.BrokerID())
	metadataLeader1.AddTopicPartition("my_topic", 0, leader1.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataLeader1)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 1
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 4

	backoffCalled := make([]int32, config.Producer.Retry.Max+1)
	config.Producer.Retry.BackoffFunc = func(retries, maxRetries int) time.Duration {
		atomic.AddInt32(&backoffCalled[retries-1], 1)
		return 0
	}
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	prodNotLeader := new(ProduceResponse)
	prodNotLeader.AddTopicPartition("my_topic", 0, ErrNotLeaderForPartition)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)

	metadataLeader2 := new(MetadataResponse)
	metadataLeader2.AddBroker(leader2.Addr(), leader2.BrokerID())
	metadataLeader2.AddTopicPartition("my_topic", 0, leader2.BrokerID(), nil, nil, nil, ErrNoError)

	leader1.Returns(prodNotLeader)
	leader1.Returns(metadataLeader2)
	leader2.Returns(prodNotLeader)
	leader2.Returns(metadataLeader1)
	leader1.Returns(prodNotLeader)
	leader1.Returns(metadataLeader1)
	leader1.Returns(prodNotLeader)
	leader1.Returns(metadataLeader2)
	leader2.Returns(prodSuccess)

	expectResults(t, producer, 1, 0)

	producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	leader2.Returns(prodSuccess)
	expectResults(t, producer, 1, 0)

	seedBroker.Close()
	leader1.Close()
	leader2.Close()
	closeProducer(t, producer)

	for i := 0; i < config.Producer.Retry.Max; i++ {
		if atomic.LoadInt32(&backoffCalled[i]) != 1 {
			t.Errorf("expected one retry attempt #%d", i)
		}
	}
	if atomic.LoadInt32(&backoffCalled[config.Producer.Retry.Max]) != 0 {
		t.Errorf("expected no retry attempt #%d", config.Producer.Retry.Max)
	}
}

// https://github.com/IBM/sarama/issues/2129
func TestAsyncProducerMultipleRetriesWithConcurrentRequests(t *testing.T) {
	// Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	// The seed broker only handles Metadata request
	seedBroker.setHandler(func(req *request) (res encoderWithHeader) {
		metadataLeader := new(MetadataResponse)
		metadataLeader.AddBroker(leader.Addr(), leader.BrokerID())
		metadataLeader.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
		return metadataLeader
	})

	// Simulate a slow broker by taking ~200ms to handle requests
	// therefore triggering the read timeout and the retry logic
	leader.setHandler(func(req *request) (res encoderWithHeader) {
		time.Sleep(200 * time.Millisecond)
		// Will likely not be read by the producer (read timeout)
		prodSuccess := new(ProduceResponse)
		prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
		return prodSuccess
	})

	config := NewTestConfig()
	// Use very short read to simulate read error on unresponsive broker
	config.Net.ReadTimeout = 50 * time.Millisecond
	// Flush every record to generate up to 5 in-flight Produce requests
	// because config.Net.MaxOpenRequests defaults to 5
	config.Producer.Flush.MaxMessages = 1
	config.Producer.Return.Successes = true
	// Reduce retries to speed up the test while keeping the default backoff
	config.Producer.Retry.Max = 1
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}

	expectResults(t, producer, 0, 10)

	seedBroker.Close()
	leader.Close()
	closeProducer(t, producer)
}

func TestAsyncProducerBrokerRestart(t *testing.T) {
	// Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	var leaderLock sync.Mutex
	metadataRequestHandlerFunc := func(req *request) (res encoderWithHeader) {
		leaderLock.Lock()
		defer leaderLock.Unlock()
		metadataLeader := new(MetadataResponse)
		metadataLeader.AddBroker(leader.Addr(), leader.BrokerID())
		metadataLeader.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
		return metadataLeader
	}

	// The seed broker only handles Metadata request in bootstrap
	seedBroker.setHandler(metadataRequestHandlerFunc)

	var emptyValues int32 = 0

	countRecordsWithEmptyValue := func(req *request) {
		preq := req.body.(*ProduceRequest)
		if batch := preq.records["my_topic"][0].RecordBatch; batch != nil {
			for _, record := range batch.Records {
				if len(record.Value) == 0 {
					atomic.AddInt32(&emptyValues, 1)
				}
			}
		}
		if batch := preq.records["my_topic"][0].MsgSet; batch != nil {
			for _, record := range batch.Messages {
				if len(record.Msg.Value) == 0 {
					atomic.AddInt32(&emptyValues, 1)
				}
			}
		}
	}

	failedProduceRequestHandlerFunc := func(req *request) (res encoderWithHeader) {
		countRecordsWithEmptyValue(req)

		time.Sleep(50 * time.Millisecond)

		prodSuccess := new(ProduceResponse)
		prodSuccess.AddTopicPartition("my_topic", 0, ErrNotLeaderForPartition)
		return prodSuccess
	}

	succeededProduceRequestHandlerFunc := func(req *request) (res encoderWithHeader) {
		countRecordsWithEmptyValue(req)

		prodSuccess := new(ProduceResponse)
		prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
		return prodSuccess
	}

	leader.SetHandlerFuncByMap(map[string]requestHandlerFunc{
		"ProduceRequest":  failedProduceRequestHandlerFunc,
		"MetadataRequest": metadataRequestHandlerFunc,
	})

	config := NewTestConfig()
	config.Producer.Retry.Backoff = 250 * time.Millisecond
	config.Producer.Flush.MaxMessages = 1
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 10

	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	pushMsg := func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
			time.Sleep(50 * time.Millisecond)
		}
	}

	wg.Add(1)
	go pushMsg()

	for i := 0; i < 3; i++ {
		time.Sleep(100 * time.Millisecond)

		wg.Add(1)
		go pushMsg()
	}

	leader.Close()
	leaderLock.Lock()
	leader = NewMockBroker(t, 2)
	leaderLock.Unlock()
	leader.SetHandlerFuncByMap(map[string]requestHandlerFunc{
		"ProduceRequest":  succeededProduceRequestHandlerFunc,
		"MetadataRequest": metadataRequestHandlerFunc,
	})

	wg.Wait()

	expectResultsWithTimeout(t, producer, 40, 0, 10*time.Second)

	seedBroker.Close()
	leader.Close()

	closeProducerWithTimeout(t, producer, 5*time.Second)

	if emptyValues := atomic.LoadInt32(&emptyValues); emptyValues > 0 {
		t.Fatalf("%d empty values", emptyValues)
	}
}

func TestAsyncProducerOutOfRetries(t *testing.T) {
	t.Skip("Enable once bug #294 is fixed.")

	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.Return.Successes = true
	config.Producer.Retry.Backoff = 0
	config.Producer.Retry.Max = 0
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}

	prodNotLeader := new(ProduceResponse)
	prodNotLeader.AddTopicPartition("my_topic", 0, ErrNotLeaderForPartition)
	leader.Returns(prodNotLeader)

	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			if !errors.Is(msg.Err, ErrNotLeaderForPartition) {
				t.Error(msg.Err)
			}
		case <-producer.Successes():
			t.Error("Unexpected success")
		}
	}

	seedBroker.Returns(metadataResponse)

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)

	expectResults(t, producer, 10, 0)

	leader.Close()
	seedBroker.Close()
	safeClose(t, producer)
}

func TestAsyncProducerRetryWithReferenceOpen(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)
	leaderAddr := leader.Addr()

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leaderAddr, leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	config := NewTestConfig()
	config.Producer.Return.Successes = true
	config.Producer.Retry.Backoff = 0
	config.Producer.Retry.Max = 1
	config.Producer.Partitioner = NewRoundRobinPartitioner
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	// prime partition 0
	producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)
	expectResults(t, producer, 1, 0)

	// prime partition 1
	producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	prodSuccess = new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 1, ErrNoError)
	leader.Returns(prodSuccess)
	expectResults(t, producer, 1, 0)

	// reboot the broker (the producer will get EOF on its existing connection)
	leader.Close()
	leader = NewMockBrokerAddr(t, 2, leaderAddr)

	// send another message on partition 0 to trigger the EOF and retry
	producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}

	// tell partition 0 to go to that broker again
	leader.Returns(metadataResponse)

	// succeed this time
	prodSuccess = new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)
	expectResults(t, producer, 1, 0)

	// shutdown
	closeProducer(t, producer)
	seedBroker.Close()
	leader.Close()
}

func TestAsyncProducerFlusherRetryCondition(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 5
	config.Producer.Return.Successes = true
	config.Producer.Retry.Backoff = 0
	config.Producer.Retry.Max = 1
	config.Producer.Partitioner = NewManualPartitioner
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	// prime partitions
	for p := int32(0); p < 2; p++ {
		for i := 0; i < 5; i++ {
			producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage), Partition: p}
		}
		prodSuccess := new(ProduceResponse)
		prodSuccess.AddTopicPartition("my_topic", p, ErrNoError)
		leader.Returns(prodSuccess)
		expectResults(t, producer, 5, 0)
	}

	// send more messages on partition 0
	for i := 0; i < 5; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage), Partition: 0}
	}
	prodNotLeader := new(ProduceResponse)
	prodNotLeader.AddTopicPartition("my_topic", 0, ErrNotLeaderForPartition)
	leader.Returns(prodNotLeader)

	time.Sleep(50 * time.Millisecond)

	// tell partition 0 to go to that broker again
	leader.Returns(metadataResponse)
	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)

	// succeed this time
	expectResults(t, producer, 5, 0)

	// put five more through
	for i := 0; i < 5; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage), Partition: 0}
	}
	prodSuccess = new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)
	expectResults(t, producer, 5, 0)

	// shutdown
	closeProducer(t, producer)
	seedBroker.Close()
	leader.Close()
}

func TestAsyncProducerRetryShutdown(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataLeader := new(MetadataResponse)
	metadataLeader.AddBroker(leader.Addr(), leader.BrokerID())
	metadataLeader.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataLeader)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.Return.Successes = true
	config.Producer.Retry.Backoff = 0
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	producer.AsyncClose()
	time.Sleep(5 * time.Millisecond) // let the shutdown goroutine kick in

	producer.Input() <- &ProducerMessage{Topic: "FOO"}
	if err := <-producer.Errors(); !errors.Is(err.Err, ErrShuttingDown) {
		t.Error(err)
	}

	prodNotLeader := new(ProduceResponse)
	prodNotLeader.AddTopicPartition("my_topic", 0, ErrNotLeaderForPartition)
	leader.Returns(prodNotLeader)

	leader.Returns(metadataLeader)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)
	expectResults(t, producer, 10, 0)

	seedBroker.Close()
	leader.Close()

	// wait for the async-closed producer to shut down fully
	for err := range producer.Errors() {
		t.Error(err)
	}
}

func TestAsyncProducerNoReturns(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataLeader := new(MetadataResponse)
	metadataLeader.AddBroker(leader.Addr(), leader.BrokerID())
	metadataLeader.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataLeader)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = false
	config.Producer.Retry.Backoff = 0
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}

	wait := make(chan bool)
	go func() {
		if err := producer.Close(); err != nil {
			t.Error(err)
		}
		close(wait)
	}()

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)

	<-wait
	seedBroker.Close()
	leader.Close()
}

func TestAsyncProducerIdempotentGoldenPath(t *testing.T) {
	broker := NewMockBroker(t, 1)

	metadataResponse := &MetadataResponse{
		Version:      4,
		ControllerID: 1,
	}
	metadataResponse.AddBroker(broker.Addr(), broker.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)
	broker.Returns(metadataResponse)

	initProducerID := &InitProducerIDResponse{
		ThrottleTime:  0,
		ProducerID:    1000,
		ProducerEpoch: 1,
	}
	broker.Returns(initProducerID)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 4
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Retry.Backoff = 0
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
	config.Version = V0_11_0_0
	producer, err := NewAsyncProducer([]string{broker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}

	prodSuccess := &ProduceResponse{
		Version:      3,
		ThrottleTime: 0,
	}
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	broker.Returns(prodSuccess)
	expectResults(t, producer, 10, 0)

	broker.Close()
	closeProducer(t, producer)
}

func TestAsyncProducerIdempotentRetryCheckBatch(t *testing.T) {
	// Logger = log.New(os.Stderr, "", log.LstdFlags)
	tests := []struct {
		name           string
		failAfterWrite bool
	}{
		{"FailAfterWrite", true},
		{"FailBeforeWrite", false},
	}

	for _, test := range tests {
		broker := NewMockBroker(t, 1)

		metadataResponse := &MetadataResponse{
			Version:      4,
			ControllerID: 1,
		}
		metadataResponse.AddBroker(broker.Addr(), broker.BrokerID())
		metadataResponse.AddTopicPartition("my_topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)

		initProducerIDResponse := &InitProducerIDResponse{
			ThrottleTime:  0,
			ProducerID:    1000,
			ProducerEpoch: 1,
		}

		prodNotLeaderResponse := &ProduceResponse{
			Version:      3,
			ThrottleTime: 0,
		}
		prodNotLeaderResponse.AddTopicPartition("my_topic", 0, ErrNotEnoughReplicas)

		prodDuplicate := &ProduceResponse{
			Version:      3,
			ThrottleTime: 0,
		}
		prodDuplicate.AddTopicPartition("my_topic", 0, ErrDuplicateSequenceNumber)

		prodOutOfSeq := &ProduceResponse{
			Version:      3,
			ThrottleTime: 0,
		}
		prodOutOfSeq.AddTopicPartition("my_topic", 0, ErrOutOfOrderSequenceNumber)

		prodSuccessResponse := &ProduceResponse{
			Version:      3,
			ThrottleTime: 0,
		}
		prodSuccessResponse.AddTopicPartition("my_topic", 0, ErrNoError)

		prodCounter := 0
		lastBatchFirstSeq := -1
		lastBatchSize := -1
		lastSequenceWrittenToDisk := -1
		handlerFailBeforeWrite := func(req *request) (res encoderWithHeader) {
			switch req.body.key() {
			case 3:
				return metadataResponse
			case 22:
				return initProducerIDResponse
			case 0:
				prodCounter++

				preq := req.body.(*ProduceRequest)
				batch := preq.records["my_topic"][0].RecordBatch
				batchFirstSeq := int(batch.FirstSequence)
				batchSize := len(batch.Records)

				if lastSequenceWrittenToDisk == batchFirstSeq-1 { // in sequence append
					if lastBatchFirstSeq == batchFirstSeq { // is a batch retry
						if lastBatchSize == batchSize { // good retry
							// mock write to disk
							lastSequenceWrittenToDisk = batchFirstSeq + batchSize - 1
							return prodSuccessResponse
						}
						t.Errorf("[%s] Retried Batch firstSeq=%d with different size old=%d new=%d", test.name, batchFirstSeq, lastBatchSize, batchSize)
						return prodOutOfSeq
					} // not a retry
					// save batch just received for future check
					lastBatchFirstSeq = batchFirstSeq
					lastBatchSize = batchSize

					if prodCounter%2 == 1 {
						if test.failAfterWrite {
							// mock write to disk
							lastSequenceWrittenToDisk = batchFirstSeq + batchSize - 1
						}
						return prodNotLeaderResponse
					}
					// mock write to disk
					lastSequenceWrittenToDisk = batchFirstSeq + batchSize - 1
					return prodSuccessResponse
				}
				if lastBatchFirstSeq == batchFirstSeq && lastBatchSize == batchSize { // is a good batch retry
					if lastSequenceWrittenToDisk == (batchFirstSeq + batchSize - 1) { // we already have the messages
						return prodDuplicate
					}
					// mock write to disk
					lastSequenceWrittenToDisk = batchFirstSeq + batchSize - 1
					return prodSuccessResponse
				} // out of sequence / bad retried batch
				if lastBatchFirstSeq == batchFirstSeq && lastBatchSize != batchSize {
					t.Errorf("[%s] Retried Batch firstSeq=%d with different size old=%d new=%d", test.name, batchFirstSeq, lastBatchSize, batchSize)
				} else if lastSequenceWrittenToDisk+1 != batchFirstSeq {
					t.Errorf("[%s] Out of sequence message lastSequence=%d new batch starts at=%d", test.name, lastSequenceWrittenToDisk, batchFirstSeq)
				} else {
					t.Errorf("[%s] Unexpected error", test.name)
				}

				return prodOutOfSeq
			}
			return nil
		}

		config := NewTestConfig()
		config.Version = V0_11_0_0
		config.Producer.Idempotent = true
		config.Net.MaxOpenRequests = 1
		config.Producer.RequiredAcks = WaitForAll
		config.Producer.Return.Successes = true
		config.Producer.Flush.Frequency = 50 * time.Millisecond
		config.Producer.Retry.Backoff = 100 * time.Millisecond

		broker.setHandler(handlerFailBeforeWrite)
		producer, err := NewAsyncProducer([]string{broker.Addr()}, config)
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 3; i++ {
			producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
		}

		go func() {
			for i := 0; i < 7; i++ {
				producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder("goroutine")}
				time.Sleep(100 * time.Millisecond)
			}
		}()

		expectResults(t, producer, 10, 0)

		broker.Close()
		closeProducer(t, producer)
	}
}

// test case for https://github.com/IBM/sarama/pull/2378
func TestAsyncProducerIdempotentRetryCheckBatch_2378(t *testing.T) {
	broker := NewMockBroker(t, 1)

	metadataResponse := &MetadataResponse{
		Version:      4,
		ControllerID: 1,
	}
	metadataResponse.AddBroker(broker.Addr(), broker.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)

	initProducerIDResponse := &InitProducerIDResponse{
		ThrottleTime:  0,
		ProducerID:    1000,
		ProducerEpoch: 1,
	}

	prodNotLeaderResponse := &ProduceResponse{
		Version:      3,
		ThrottleTime: 0,
	}
	prodNotLeaderResponse.AddTopicPartition("my_topic", 0, ErrNotEnoughReplicas)

	handlerFailBeforeWrite := func(req *request) (res encoderWithHeader) {
		switch req.body.key() {
		case 3:
			return metadataResponse
		case 22:
			return initProducerIDResponse
		case 0: // for msg, always return error to trigger retryBatch
			return prodNotLeaderResponse
		}
		return nil
	}

	config := NewTestConfig()
	config.Version = V0_11_0_0
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
	config.Producer.Retry.Max = 1 // set max retry to 1
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.Retry.Backoff = 100 * time.Millisecond

	broker.setHandler(handlerFailBeforeWrite)
	producer, err := NewAsyncProducer([]string{broker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}

	go func() {
		for i := 0; i < 7; i++ {
			producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder("goroutine")}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// this will block until 5 minutes timeout before pr 2378 merge
	expectResults(t, producer, 0, 10)

	broker.Close()
	closeProducer(t, producer)
}

func TestAsyncProducerIdempotentErrorOnOutOfSeq(t *testing.T) {
	broker := NewMockBroker(t, 1)

	metadataResponse := &MetadataResponse{
		Version:      4,
		ControllerID: 1,
	}
	metadataResponse.AddBroker(broker.Addr(), broker.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)
	broker.Returns(metadataResponse)

	initProducerID := &InitProducerIDResponse{
		ThrottleTime:  0,
		ProducerID:    1000,
		ProducerEpoch: 1,
	}
	broker.Returns(initProducerID)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 400000
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Retry.Backoff = 0
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
	config.Version = V0_11_0_0

	producer, err := NewAsyncProducer([]string{broker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}

	prodOutOfSeq := &ProduceResponse{
		Version:      3,
		ThrottleTime: 0,
	}
	prodOutOfSeq.AddTopicPartition("my_topic", 0, ErrOutOfOrderSequenceNumber)
	broker.Returns(prodOutOfSeq)
	expectResults(t, producer, 0, 10)

	broker.Close()
	closeProducer(t, producer)
}

func TestAsyncProducerIdempotentEpochRollover(t *testing.T) {
	broker := NewMockBroker(t, 1)
	defer broker.Close()

	metadataResponse := &MetadataResponse{
		Version:      4,
		ControllerID: 1,
	}
	metadataResponse.AddBroker(broker.Addr(), broker.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)
	broker.Returns(metadataResponse)

	initProducerID := &InitProducerIDResponse{
		ThrottleTime:  0,
		ProducerID:    1000,
		ProducerEpoch: 1,
	}
	broker.Returns(initProducerID)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.Flush.Frequency = 10 * time.Millisecond
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 1 // This test needs to exercise what happens when retries exhaust
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Retry.Backoff = 0
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
	config.Version = V0_11_0_0

	producer, err := NewAsyncProducer([]string{broker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeProducer(t, producer)

	producer.Input() <- &ProducerMessage{Topic: "my_topic", Value: StringEncoder("hello")}
	prodError := &ProduceResponse{
		Version:      3,
		ThrottleTime: 0,
	}
	prodError.AddTopicPartition("my_topic", 0, ErrBrokerNotAvailable)
	broker.Returns(prodError)
	<-producer.Errors()

	lastReqRes := broker.history[len(broker.history)-1]
	lastProduceBatch := lastReqRes.Request.(*ProduceRequest).records["my_topic"][0].RecordBatch
	if lastProduceBatch.FirstSequence != 0 {
		t.Error("first sequence not zero")
	}
	if lastProduceBatch.ProducerEpoch != 1 {
		t.Error("first epoch was not one")
	}

	// Now if we produce again, the epoch should have rolled over.
	producer.Input() <- &ProducerMessage{Topic: "my_topic", Value: StringEncoder("hello")}
	broker.Returns(prodError)
	<-producer.Errors()

	lastReqRes = broker.history[len(broker.history)-1]
	lastProduceBatch = lastReqRes.Request.(*ProduceRequest).records["my_topic"][0].RecordBatch
	if lastProduceBatch.FirstSequence != 0 {
		t.Error("second sequence not zero")
	}
	if lastProduceBatch.ProducerEpoch <= 1 {
		t.Error("second epoch was not > 1")
	}
}

// TestAsyncProducerIdempotentEpochExhaustion ensures that producer requests
// a new producerID when producerEpoch is exhausted
func TestAsyncProducerIdempotentEpochExhaustion(t *testing.T) {
	broker := NewMockBroker(t, 1)
	defer broker.Close()

	var (
		initialProducerID = int64(1000)
		newProducerID     = initialProducerID + 1
	)

	metadataResponse := &MetadataResponse{
		Version:      4,
		ControllerID: 1,
	}
	metadataResponse.AddBroker(broker.Addr(), broker.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)
	broker.Returns(metadataResponse)

	initProducerID := &InitProducerIDResponse{
		ThrottleTime:  0,
		ProducerID:    initialProducerID,
		ProducerEpoch: math.MaxInt16, // Mock ProducerEpoch at the exhaustion point
	}
	broker.Returns(initProducerID)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.Flush.Frequency = 10 * time.Millisecond
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 1 // This test needs to exercise what happens when retries exhaust
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Retry.Backoff = 0
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
	config.Version = V0_11_0_0

	producer, err := NewAsyncProducer([]string{broker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeProducer(t, producer)

	producer.Input() <- &ProducerMessage{Topic: "my_topic", Value: StringEncoder("hello")}
	prodError := &ProduceResponse{
		Version:      3,
		ThrottleTime: 0,
	}
	prodError.AddTopicPartition("my_topic", 0, ErrBrokerNotAvailable)
	broker.Returns(prodError)
	broker.Returns(&InitProducerIDResponse{
		ProducerID: newProducerID,
	})

	<-producer.Errors()

	lastProduceReqRes := broker.history[len(broker.history)-2] // last is InitProducerIDRequest
	lastProduceBatch := lastProduceReqRes.Request.(*ProduceRequest).records["my_topic"][0].RecordBatch
	if lastProduceBatch.FirstSequence != 0 {
		t.Error("first sequence not zero")
	}
	if lastProduceBatch.ProducerEpoch <= 1 {
		t.Error("first epoch was not at exhaustion point")
	}

	// Now we should produce with a new ProducerID
	producer.Input() <- &ProducerMessage{Topic: "my_topic", Value: StringEncoder("hello")}
	broker.Returns(prodError)
	<-producer.Errors()

	lastProduceReqRes = broker.history[len(broker.history)-1]
	lastProduceBatch = lastProduceReqRes.Request.(*ProduceRequest).records["my_topic"][0].RecordBatch
	if lastProduceBatch.ProducerID != newProducerID || lastProduceBatch.ProducerEpoch != 0 {
		t.Error("producer did not requested a new producerID")
	}
}

// TestBrokerProducerShutdown ensures that a call to shutdown stops the
// brokerProducer run() loop and doesn't leak any goroutines
//
//nolint:paralleltest
func TestBrokerProducerShutdown(t *testing.T) {
	defer leaktest.Check(t)()
	metrics.UseNilMetrics = true // disable Sarama's go-metrics library
	defer func() {
		metrics.UseNilMetrics = false
	}()

	mockBroker := NewMockBroker(t, 1)
	metadataResponse := &MetadataResponse{}
	metadataResponse.AddBroker(mockBroker.Addr(), mockBroker.BrokerID())
	metadataResponse.AddTopicPartition(
		"my_topic", 0, mockBroker.BrokerID(), nil, nil, nil, ErrNoError)
	mockBroker.Returns(metadataResponse)

	producer, err := NewAsyncProducer([]string{mockBroker.Addr()}, NewTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	broker := &Broker{
		addr: mockBroker.Addr(),
		id:   mockBroker.BrokerID(),
	}
	// Starts various goroutines in newBrokerProducer
	bp := producer.(*asyncProducer).getBrokerProducer(broker)
	// Initiate the shutdown of all of them
	producer.(*asyncProducer).unrefBrokerProducer(broker, bp)

	_ = producer.Close()
	mockBroker.Close()
}

func testProducerInterceptor(
	t *testing.T,
	interceptors []ProducerInterceptor,
	expectationFn func(*testing.T, int, *ProducerMessage),
) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)
	metadataLeader := new(MetadataResponse)
	metadataLeader.AddBroker(leader.Addr(), leader.BrokerID())
	metadataLeader.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataLeader)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.Return.Successes = true
	config.Producer.Interceptors = interceptors
	producer, err := NewAsyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)

	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
		case msg := <-producer.Successes():
			expectationFn(t, i, msg)
		}
	}

	closeProducer(t, producer)
	leader.Close()
	seedBroker.Close()
}

func TestAsyncProducerInterceptors(t *testing.T) {
	tests := []struct {
		name          string
		interceptors  []ProducerInterceptor
		expectationFn func(*testing.T, int, *ProducerMessage)
	}{
		{
			name:         "intercept messages",
			interceptors: []ProducerInterceptor{&appendInterceptor{i: 0}},
			expectationFn: func(t *testing.T, i int, msg *ProducerMessage) {
				v, _ := msg.Value.Encode()
				expected := TestMessage + strconv.Itoa(i)
				if string(v) != expected {
					t.Errorf("Interceptor should have incremented the value, got %s, expected %s", v, expected)
				}
			},
		},
		{
			name:         "interceptor chain",
			interceptors: []ProducerInterceptor{&appendInterceptor{i: 0}, &appendInterceptor{i: 1000}},
			expectationFn: func(t *testing.T, i int, msg *ProducerMessage) {
				v, _ := msg.Value.Encode()
				expected := TestMessage + strconv.Itoa(i) + strconv.Itoa(i+1000)
				if string(v) != expected {
					t.Errorf("Interceptor should have incremented the value, got %s, expected %s", v, expected)
				}
			},
		},
		{
			name:         "interceptor chain with one interceptor failing",
			interceptors: []ProducerInterceptor{&appendInterceptor{i: -1}, &appendInterceptor{i: 1000}},
			expectationFn: func(t *testing.T, i int, msg *ProducerMessage) {
				v, _ := msg.Value.Encode()
				expected := TestMessage + strconv.Itoa(i+1000)
				if string(v) != expected {
					t.Errorf("Interceptor should have incremented the value, got %s, expected %s", v, expected)
				}
			},
		},
		{
			name:         "interceptor chain with all interceptors failing",
			interceptors: []ProducerInterceptor{&appendInterceptor{i: -1}, &appendInterceptor{i: -1}},
			expectationFn: func(t *testing.T, i int, msg *ProducerMessage) {
				v, _ := msg.Value.Encode()
				expected := TestMessage
				if string(v) != expected {
					t.Errorf("Interceptor should have not changed the value, got %s, expected %s", v, expected)
				}
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			testProducerInterceptor(t, tt.interceptors, tt.expectationFn)
		})
	}
}

func TestProducerError(t *testing.T) {
	t.Parallel()
	err := ProducerError{Err: ErrOutOfBrokers}
	if !errors.Is(err, ErrOutOfBrokers) {
		t.Error("unexpected errors.Is")
	}
}

func TestTxmngInitProducerId(t *testing.T) {
	broker := NewMockBroker(t, 1)
	defer broker.Close()

	metadataLeader := new(MetadataResponse)
	metadataLeader.Version = 4
	metadataLeader.AddBroker(broker.Addr(), broker.BrokerID())
	broker.Returns(metadataLeader)

	config := NewTestConfig()
	config.Producer.Idempotent = true
	config.Version = V0_11_0_0
	config.Producer.RequiredAcks = WaitForAll
	config.Net.MaxOpenRequests = 1

	client, err := NewClient([]string{broker.Addr()}, config)
	require.NoError(t, err)
	defer client.Close()

	producerIdResponse := &InitProducerIDResponse{
		Err:           ErrNoError,
		ProducerID:    1,
		ProducerEpoch: 0,
	}
	broker.Returns(producerIdResponse)

	txmng, err := newTransactionManager(config, client)
	require.NoError(t, err)

	require.Equal(t, int64(1), txmng.producerID)
	require.Equal(t, int16(0), txmng.producerEpoch)
}

func TestTxnProduceBumpEpoch(t *testing.T) {
	broker := NewMockBroker(t, 1)
	defer broker.Close()

	config := NewTestConfig()
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "test"
	config.Version = V2_6_0_0
	config.Producer.RequiredAcks = WaitForAll
	config.Net.MaxOpenRequests = 1
	config.Producer.Return.Errors = false

	config.ApiVersionsRequest = false

	metadataLeader := new(MetadataResponse)
	metadataLeader.Version = 9
	metadataLeader.ControllerID = broker.brokerID
	metadataLeader.AddBroker(broker.Addr(), broker.BrokerID())
	metadataLeader.AddTopic("test-topic", ErrNoError)
	metadataLeader.AddTopicPartition("test-topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)
	broker.Returns(metadataLeader)

	client, err := NewClient([]string{broker.Addr()}, config)
	require.NoError(t, err)
	defer client.Close()

	findCoordinatorResponse := FindCoordinatorResponse{
		Coordinator: client.Brokers()[0],
		Err:         ErrNoError,
		Version:     1,
	}
	broker.Returns(&findCoordinatorResponse)

	producerIdResponse := &InitProducerIDResponse{
		Err:           ErrNoError,
		ProducerID:    1000,
		ProducerEpoch: 0,
		Version:       3,
	}
	broker.Returns(producerIdResponse)

	ap, err := NewAsyncProducerFromClient(client)
	producer := ap.(*asyncProducer)
	require.NoError(t, err)
	defer ap.Close()
	require.Equal(t, int64(1000), producer.txnmgr.producerID)
	require.Equal(t, int16(0), producer.txnmgr.producerEpoch)

	addPartitionsToTxnResponse := &AddPartitionsToTxnResponse{
		Errors: map[string][]*PartitionError{
			"test-topic": {
				{
					Partition: 0,
				},
			},
		},
	}
	broker.Returns(addPartitionsToTxnResponse)

	produceResponse := new(ProduceResponse)
	produceResponse.Version = 7
	produceResponse.AddTopicPartition("test-topic", 0, ErrOutOfOrderSequenceNumber)
	broker.Returns(produceResponse)

	err = producer.BeginTxn()
	require.NoError(t, err)

	producer.Input() <- &ProducerMessage{Topic: "test-topic", Key: nil, Value: StringEncoder(TestMessage)}

	// Force send
	producer.inFlight.Add(1)
	producer.Input() <- &ProducerMessage{flags: shutdown}
	producer.inFlight.Wait()

	err = producer.CommitTxn()
	require.Error(t, err)
	require.Equal(t, ProducerTxnFlagInError|ProducerTxnFlagAbortableError, producer.txnmgr.status)

	err = producer.CommitTxn()
	require.Error(t, err)
	require.Equal(t, ProducerTxnFlagInError|ProducerTxnFlagAbortableError, producer.txnmgr.status)

	endTxnResponse := &EndTxnResponse{
		Err: ErrNoError,
	}
	broker.Returns(endTxnResponse)

	producerBumpIdResponse := &InitProducerIDResponse{
		Err:           ErrNoError,
		ProducerID:    1000,
		ProducerEpoch: 1,
		Version:       3,
	}
	broker.Returns(producerBumpIdResponse)

	err = producer.AbortTxn()
	require.NoError(t, err)
	require.Equal(t, ProducerTxnFlagReady, producer.txnmgr.status)
	require.Equal(t, int64(1000), producer.txnmgr.producerID)
	require.Equal(t, int16(1), producer.txnmgr.producerEpoch)
}

func TestTxnProduceRecordWithCommit(t *testing.T) {
	broker := NewMockBroker(t, 1)
	defer broker.Close()

	config := NewTestConfig()
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "test"
	config.Version = V0_11_0_0
	config.Producer.RequiredAcks = WaitForAll
	config.Net.MaxOpenRequests = 1

	metadataLeader := new(MetadataResponse)
	metadataLeader.Version = 4
	metadataLeader.ControllerID = broker.brokerID
	metadataLeader.AddBroker(broker.Addr(), broker.BrokerID())
	metadataLeader.AddTopic("test-topic", ErrNoError)
	metadataLeader.AddTopicPartition("test-topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)
	broker.Returns(metadataLeader)

	client, err := NewClient([]string{broker.Addr()}, config)
	require.NoError(t, err)
	defer client.Close()

	findCoordinatorResponse := FindCoordinatorResponse{
		Coordinator: client.Brokers()[0],
		Err:         ErrNoError,
		Version:     1,
	}
	broker.Returns(&findCoordinatorResponse)

	producerIdResponse := &InitProducerIDResponse{
		Err:           ErrNoError,
		ProducerID:    1,
		ProducerEpoch: 0,
	}
	broker.Returns(producerIdResponse)

	ap, err := NewAsyncProducerFromClient(client)
	producer := ap.(*asyncProducer)
	require.NoError(t, err)
	defer ap.Close()

	addPartitionsToTxnResponse := &AddPartitionsToTxnResponse{
		Errors: map[string][]*PartitionError{
			"test-topic": {
				{
					Partition: 0,
				},
			},
		},
	}
	broker.Returns(addPartitionsToTxnResponse)

	produceResponse := new(ProduceResponse)
	produceResponse.Version = 3
	produceResponse.AddTopicPartition("test-topic", 0, ErrNoError)
	broker.Returns(produceResponse)

	endTxnResponse := &EndTxnResponse{
		Err: ErrNoError,
	}
	broker.Returns(endTxnResponse)

	require.Equal(t, ProducerTxnFlagReady, producer.txnmgr.status)

	err = producer.BeginTxn()
	require.NoError(t, err)
	require.Equal(t, ProducerTxnFlagInTransaction, producer.txnmgr.status)

	producer.Input() <- &ProducerMessage{Topic: "test-topic", Key: nil, Value: StringEncoder(TestMessage)}
	err = producer.CommitTxn()
	require.NoError(t, err)
	require.Equal(t, ProducerTxnFlagReady, producer.txnmgr.status)
}

func TestTxnProduceBatchAddPartition(t *testing.T) {
	broker := NewMockBroker(t, 1)
	defer broker.Close()

	config := NewTestConfig()
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "test"
	config.Version = V0_11_0_0
	config.Producer.RequiredAcks = WaitForAll
	config.Net.MaxOpenRequests = 1

	config.Producer.Retry.Max = 1
	config.Producer.Flush.Messages = 3
	config.Producer.Flush.Frequency = 30 * time.Second
	config.Producer.Flush.Bytes = 1 << 12
	config.Producer.Partitioner = NewManualPartitioner

	metadataLeader := new(MetadataResponse)
	metadataLeader.Version = 4
	metadataLeader.ControllerID = broker.brokerID
	metadataLeader.AddBroker(broker.Addr(), broker.BrokerID())
	metadataLeader.AddTopic("test-topic", ErrNoError)
	metadataLeader.AddTopicPartition("test-topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)
	metadataLeader.AddTopicPartition("test-topic", 1, broker.BrokerID(), nil, nil, nil, ErrNoError)
	metadataLeader.AddTopicPartition("test-topic", 2, broker.BrokerID(), nil, nil, nil, ErrNoError)
	broker.Returns(metadataLeader)

	client, err := NewClient([]string{broker.Addr()}, config)
	require.NoError(t, err)
	defer client.Close()

	findCoordinatorResponse := FindCoordinatorResponse{
		Coordinator: client.Brokers()[0],
		Err:         ErrNoError,
		Version:     1,
	}
	broker.Returns(&findCoordinatorResponse)

	producerIdResponse := &InitProducerIDResponse{
		Err:           ErrNoError,
		ProducerID:    1,
		ProducerEpoch: 0,
	}
	broker.Returns(producerIdResponse)

	ap, err := NewAsyncProducerFromClient(client)
	producer := ap.(*asyncProducer)
	require.NoError(t, err)
	defer ap.Close()

	go func() {
		for err := range producer.Errors() {
			require.NoError(t, err)
		}
	}()

	broker.Returns(&AddPartitionsToTxnResponse{
		Errors: map[string][]*PartitionError{
			"test-topic": {
				{
					Partition: 0,
					Err:       ErrNoError,
				},
				{
					Partition: 1,
					Err:       ErrNoError,
				},
				{
					Partition: 2,
					Err:       ErrNoError,
				},
			},
		},
	})

	produceResponse := new(ProduceResponse)
	produceResponse.Version = 3
	produceResponse.AddTopicPartition("test-topic", 0, ErrNoError)
	produceResponse.AddTopicPartition("test-topic", 1, ErrNoError)
	produceResponse.AddTopicPartition("test-topic", 2, ErrNoError)
	broker.Returns(produceResponse)

	endTxnResponse := &EndTxnResponse{
		Err: ErrNoError,
	}
	broker.Returns(endTxnResponse)

	require.Equal(t, ProducerTxnFlagReady, producer.txnmgr.status)

	err = producer.BeginTxn()
	require.NoError(t, err)
	require.Equal(t, ProducerTxnFlagInTransaction, producer.txnmgr.status)

	producer.Input() <- &ProducerMessage{Topic: "test-topic", Partition: 0, Key: nil, Value: StringEncoder("partition-0")}
	producer.Input() <- &ProducerMessage{Topic: "test-topic", Partition: 1, Key: nil, Value: StringEncoder("partition-1")}
	producer.Input() <- &ProducerMessage{Topic: "test-topic", Partition: 2, Key: nil, Value: StringEncoder("partition-2")}

	err = producer.CommitTxn()
	require.NoError(t, err)
	require.Equal(t, ProducerTxnFlagReady, producer.txnmgr.status)

	produceExchange := broker.History()[len(broker.History())-2]
	produceRequest := produceExchange.Request.(*ProduceRequest)
	require.Equal(t, 3, len(produceRequest.records["test-topic"]))

	addPartitionExchange := broker.History()[len(broker.History())-3]
	addpartitionRequest := addPartitionExchange.Request.(*AddPartitionsToTxnRequest)
	require.Equal(t, 3, len(addpartitionRequest.TopicPartitions["test-topic"]))
	require.Contains(t, addpartitionRequest.TopicPartitions["test-topic"], int32(0))
	require.Contains(t, addpartitionRequest.TopicPartitions["test-topic"], int32(1))
	require.Contains(t, addpartitionRequest.TopicPartitions["test-topic"], int32(2))
}

func TestTxnProduceRecordWithAbort(t *testing.T) {
	broker := NewMockBroker(t, 1)
	defer broker.Close()

	config := NewTestConfig()
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "test"
	config.Version = V0_11_0_0
	config.Producer.RequiredAcks = WaitForAll
	config.Net.MaxOpenRequests = 1

	metadataLeader := new(MetadataResponse)
	metadataLeader.Version = 4
	metadataLeader.ControllerID = broker.brokerID
	metadataLeader.AddBroker(broker.Addr(), broker.BrokerID())
	metadataLeader.AddTopic("test-topic", ErrNoError)
	metadataLeader.AddTopicPartition("test-topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)
	broker.Returns(metadataLeader)

	client, err := NewClient([]string{broker.Addr()}, config)
	require.NoError(t, err)
	defer client.Close()

	findCoordinatorResponse := FindCoordinatorResponse{
		Coordinator: client.Brokers()[0],
		Err:         ErrNoError,
		Version:     1,
	}
	broker.Returns(&findCoordinatorResponse)

	producerIdResponse := &InitProducerIDResponse{
		Err:           ErrNoError,
		ProducerID:    1,
		ProducerEpoch: 0,
	}
	broker.Returns(producerIdResponse)

	ap, err := NewAsyncProducerFromClient(client)
	producer := ap.(*asyncProducer)
	require.NoError(t, err)
	defer ap.Close()

	broker.Returns(&AddPartitionsToTxnResponse{
		Errors: map[string][]*PartitionError{
			"test-topic": {
				{
					Partition: 0,
					Err:       ErrNoError,
				},
			},
		},
	})

	produceResponse := new(ProduceResponse)
	produceResponse.Version = 3
	produceResponse.AddTopicPartition("test-topic", 0, ErrNoError)
	broker.Returns(produceResponse)

	endTxnResponse := &EndTxnResponse{
		Err: ErrNoError,
	}
	broker.Returns(endTxnResponse)

	require.Equal(t, ProducerTxnFlagReady, producer.txnmgr.status)

	err = producer.BeginTxn()
	require.NoError(t, err)
	require.Equal(t, ProducerTxnFlagInTransaction, producer.txnmgr.status)

	producer.Input() <- &ProducerMessage{Topic: "test-topic", Key: nil, Value: StringEncoder(TestMessage)}
	err = producer.AbortTxn()
	require.NoError(t, err)
	require.Equal(t, ProducerTxnFlagReady, producer.txnmgr.status)
}

func TestTxnCanAbort(t *testing.T) {
	broker := NewMockBroker(t, 1)
	defer broker.Close()

	config := NewTestConfig()
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "test"
	config.Version = V0_11_0_0
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Return.Errors = false
	config.Producer.Return.Successes = true
	config.Producer.Retry.Backoff = 0
	config.Producer.Flush.Messages = 1
	config.Producer.Retry.Max = 1
	config.Net.MaxOpenRequests = 1

	metadataLeader := new(MetadataResponse)
	metadataLeader.Version = 4
	metadataLeader.ControllerID = broker.brokerID
	metadataLeader.AddBroker(broker.Addr(), broker.BrokerID())
	metadataLeader.AddTopic("test-topic", ErrNoError)
	metadataLeader.AddTopic("test-topic-2", ErrNoError)
	metadataLeader.AddTopicPartition("test-topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)
	metadataLeader.AddTopicPartition("test-topic-2", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)
	broker.Returns(metadataLeader)

	client, err := NewClient([]string{broker.Addr()}, config)
	require.NoError(t, err)
	defer client.Close()

	findCoordinatorResponse := FindCoordinatorResponse{
		Coordinator: client.Brokers()[0],
		Err:         ErrNoError,
		Version:     1,
	}
	broker.Returns(&findCoordinatorResponse)

	producerIdResponse := &InitProducerIDResponse{
		Err:           ErrNoError,
		ProducerID:    1,
		ProducerEpoch: 0,
	}
	broker.Returns(producerIdResponse)

	ap, err := NewAsyncProducerFromClient(client)
	producer := ap.(*asyncProducer)
	require.NoError(t, err)
	defer ap.Close()

	broker.Returns(&AddPartitionsToTxnResponse{
		Errors: map[string][]*PartitionError{
			"test-topic-2": {
				{
					Partition: 0,
					Err:       ErrNoError,
				},
			},
		},
	})

	produceResponse := new(ProduceResponse)
	produceResponse.Version = 3
	produceResponse.AddTopicPartition("test-topic-2", 0, ErrNoError)
	broker.Returns(produceResponse)

	broker.Returns(&AddPartitionsToTxnResponse{
		Errors: map[string][]*PartitionError{
			"test-topic": {
				{
					Partition: 0,
					Err:       ErrTopicAuthorizationFailed,
				},
			},
		},
	})

	// now broker is closed due to error. will now reopen it
	broker.Returns(metadataLeader)

	endTxnResponse := &EndTxnResponse{
		Err: ErrNoError,
	}
	broker.Returns(endTxnResponse)

	require.Equal(t, ProducerTxnFlagReady, producer.txnmgr.status)

	err = producer.BeginTxn()
	require.NoError(t, err)
	require.Equal(t, ProducerTxnFlagInTransaction, producer.txnmgr.status)

	producer.Input() <- &ProducerMessage{Topic: "test-topic-2", Partition: 0, Key: nil, Value: StringEncoder(TestMessage)}
	<-producer.Successes()

	producer.Input() <- &ProducerMessage{Topic: "test-topic", Partition: 0, Key: nil, Value: StringEncoder(TestMessage)}

	err = producer.CommitTxn()
	require.Error(t, err)
	require.NotEqual(t, producer.txnmgr.status&ProducerTxnFlagAbortableError, 0)

	err = producer.AbortTxn()
	require.NoError(t, err)
}

func TestProducerRetryBufferLimits(t *testing.T) {
	broker := NewMockBroker(t, 1)
	defer broker.Close()
	topic := "test-topic"

	metadataRequestHandlerFunc := func(req *request) (res encoderWithHeader) {
		r := new(MetadataResponse)
		r.AddBroker(broker.Addr(), broker.BrokerID())
		r.AddTopicPartition(topic, 0, broker.BrokerID(), nil, nil, nil, ErrNoError)
		return r
	}

	produceRequestHandlerFunc := func(req *request) (res encoderWithHeader) {
		r := new(ProduceResponse)
		r.AddTopicPartition(topic, 0, ErrNotLeaderForPartition)
		return r
	}

	broker.SetHandlerFuncByMap(map[string]requestHandlerFunc{
		"ProduceRequest":  produceRequestHandlerFunc,
		"MetadataRequest": metadataRequestHandlerFunc,
	})

	tests := []struct {
		name            string
		configureBuffer func(*Config)
		messageSize     int
		numMessages     int
	}{
		{
			name: "MaxBufferLength",
			configureBuffer: func(config *Config) {
				config.Producer.Flush.MaxMessages = 1
				config.Producer.Retry.MaxBufferLength = minFunctionalRetryBufferLength
			},
			messageSize: 1, // Small message size
			numMessages: 10000,
		},
		{
			name: "MaxBufferBytes",
			configureBuffer: func(config *Config) {
				config.Producer.Flush.MaxMessages = 1
				config.Producer.Retry.MaxBufferBytes = minFunctionalRetryBufferBytes
			},
			messageSize: 950 * 1024, // 950 KB
			numMessages: 1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewTestConfig()
			config.Producer.Return.Successes = true
			tt.configureBuffer(config)

			producer, err := NewAsyncProducer([]string{broker.Addr()}, config)
			if err != nil {
				t.Fatal(err)
			}

			var (
				wg                        sync.WaitGroup
				successes, producerErrors int
				errorFound                bool
			)

			wg.Add(1)
			go func() {
				defer wg.Done()
				for range producer.Successes() {
					successes++
				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				for errMsg := range producer.Errors() {
					if errors.Is(errMsg.Err, ErrProducerRetryBufferOverflow) {
						errorFound = true
					}
					producerErrors++
				}
			}()

			longString := strings.Repeat("a", tt.messageSize)
			val := StringEncoder(longString)

			for i := 0; i < tt.numMessages; i++ {
				msg := &ProducerMessage{
					Topic: topic,
					Value: val,
				}
				producer.Input() <- msg
			}

			producer.AsyncClose()
			wg.Wait()

			assert.Equal(t, successes+producerErrors, tt.numMessages, "Expected all messages to be processed")
			assert.True(t, errorFound, "Expected at least one error matching ErrProducerRetryBufferOverflow")
		})
	}
}

// This example shows how to use the producer while simultaneously
// reading the Errors channel to know about any failures.
func ExampleAsyncProducer_select() {
	producer, err := NewAsyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enqueued, producerErrors int
ProducerLoop:
	for {
		select {
		case producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder("testing 123")}:
			enqueued++
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			producerErrors++
		case <-signals:
			break ProducerLoop
		}
	}

	log.Printf("Enqueued: %d; errors: %d\n", enqueued, producerErrors)
}

// This example shows how to use the producer with separate goroutines
// reading from the Successes and Errors channels. Note that in order
// for the Successes channel to be populated, you have to set
// config.Producer.Return.Successes to true.
func ExampleAsyncProducer_goroutines() {
	config := NewTestConfig()
	config.Producer.Return.Successes = true
	producer, err := NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var (
		wg                                  sync.WaitGroup
		enqueued, successes, producerErrors int
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range producer.Successes() {
			successes++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println(err)
			producerErrors++
		}
	}()

ProducerLoop:
	for {
		message := &ProducerMessage{Topic: "my_topic", Value: StringEncoder("testing 123")}
		select {
		case producer.Input() <- message:
			enqueued++

		case <-signals:
			producer.AsyncClose() // Trigger a shutdown of the producer.
			break ProducerLoop
		}
	}

	wg.Wait()

	log.Printf("Successfully produced: %d; errors: %d\n", successes, producerErrors)
}

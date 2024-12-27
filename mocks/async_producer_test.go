//go:build !functional

package mocks

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/IBM/sarama"
)

func generateRegexpChecker(re string) func([]byte) error {
	return func(val []byte) error {
		matched, err := regexp.MatchString(re, string(val))
		if err != nil {
			return errors.New("Error while trying to match the input message with the expected pattern: " + err.Error())
		}
		if !matched {
			return fmt.Errorf("No match between input value \"%s\" and expected pattern \"%s\"", val, re)
		}
		return nil
	}
}

type testReporterMock struct {
	errors []string
}

func newTestReporterMock() *testReporterMock {
	return &testReporterMock{errors: make([]string, 0)}
}

func (trm *testReporterMock) Errorf(format string, args ...interface{}) {
	trm.errors = append(trm.errors, fmt.Sprintf(format, args...))
}

func TestMockAsyncProducerImplementsAsyncProducerInterface(t *testing.T) {
	var mp interface{} = &AsyncProducer{}
	if _, ok := mp.(sarama.AsyncProducer); !ok {
		t.Error("The mock producer should implement the sarama.Producer interface.")
	}
}

func TestProducerReturnsExpectationsToChannels(t *testing.T) {
	config := NewTestConfig()
	config.Producer.Return.Successes = true
	mp := NewAsyncProducer(t, config).
		ExpectInputAndSucceed().
		ExpectInputAndSucceed().
		ExpectInputAndFail(sarama.ErrOutOfBrokers)

	mp.Input() <- &sarama.ProducerMessage{Topic: "test 1"}
	mp.Input() <- &sarama.ProducerMessage{Topic: "test 2"}
	mp.Input() <- &sarama.ProducerMessage{Topic: "test 3"}

	msg1 := <-mp.Successes()
	msg2 := <-mp.Successes()
	err1 := <-mp.Errors()

	if msg1.Topic != "test 1" {
		t.Error("Expected message 1 to be returned first")
	}

	if msg2.Topic != "test 2" {
		t.Error("Expected message 2 to be returned second")
	}

	if err1.Msg.Topic != "test 3" || !errors.Is(err1, sarama.ErrOutOfBrokers) {
		t.Error("Expected message 3 to be returned as error")
	}

	if err := mp.Close(); err != nil {
		t.Error(err)
	}
}

func TestProducerWithTooFewExpectations(t *testing.T) {
	trm := newTestReporterMock()
	mp := NewAsyncProducer(trm, nil)
	mp.ExpectInputAndSucceed()

	mp.Input() <- &sarama.ProducerMessage{Topic: "test"}
	mp.Input() <- &sarama.ProducerMessage{Topic: "test"}

	if err := mp.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 1 {
		t.Error("Expected to report an error")
	}
}

func TestProducerWithTooManyExpectations(t *testing.T) {
	trm := newTestReporterMock()
	mp := NewAsyncProducer(trm, nil).
		ExpectInputAndSucceed().
		ExpectInputAndFail(sarama.ErrOutOfBrokers)

	mp.Input() <- &sarama.ProducerMessage{Topic: "test"}
	if err := mp.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 1 {
		t.Error("Expected to report an error")
	}
}

func TestProducerFailTxn(t *testing.T) {
	config := NewTestConfig()
	config.Producer.Transaction.ID = "test"
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Backoff = 0
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
	config.Version = sarama.V0_11_0_0

	trm := newTestReporterMock()
	mp := NewAsyncProducer(trm, config)

	mp.Input() <- &sarama.ProducerMessage{Topic: "test"}

	_ = mp.Close()

	if len(trm.errors) != 1 {
		t.Error("must have fail with txn begin error")
	}
}

func TestProducerWithTxn(t *testing.T) {
	config := NewTestConfig()
	config.Producer.Transaction.ID = "test"
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Backoff = 0
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
	config.Version = sarama.V0_11_0_0

	trm := newTestReporterMock()
	mp := NewAsyncProducer(trm, config).
		ExpectInputAndSucceed()

	if !mp.IsTransactional() {
		t.Error("producer must be transactional")
	}

	if err := mp.BeginTxn(); err != nil {
		t.Error(err)
	}

	if mp.TxnStatus()&sarama.ProducerTxnFlagInTransaction == 0 {
		t.Error("transaction must be started")
	}

	mp.Input() <- &sarama.ProducerMessage{Topic: "test"}

	if err := mp.AddMessageToTxn(&sarama.ConsumerMessage{
		Topic:     "original-topic",
		Partition: 0,
		Offset:    123,
	}, "test-group", nil); err != nil {
		t.Error(err)
	}

	if err := mp.AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata{
		"original-topic": {
			{
				Partition: 1,
				Offset:    321,
			},
		},
	}, "test-group"); err != nil {
		t.Error(err)
	}

	if err := mp.CommitTxn(); err != nil {
		t.Error(err)
	}

	if err := mp.Close(); err != nil {
		t.Error(err)
	}
}

func TestProducerWithCheckerFunction(t *testing.T) {
	trm := newTestReporterMock()
	mp := NewAsyncProducer(trm, nil).
		ExpectInputWithCheckerFunctionAndSucceed(generateRegexpChecker("^tes")).
		ExpectInputWithCheckerFunctionAndSucceed(generateRegexpChecker("^tes$"))

	mp.Input() <- &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("test")}
	mp.Input() <- &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("test")}
	if err := mp.Close(); err != nil {
		t.Error(err)
	}

	if len(mp.Errors()) != 1 {
		t.Error("Expected to report an error")
	}

	err1 := <-mp.Errors()
	if !strings.HasPrefix(err1.Err.Error(), "No match") {
		t.Error("Expected to report a value check error, found: ", err1.Err)
	}
}

func TestProducerWithBrokenPartitioner(t *testing.T) {
	trm := newTestReporterMock()
	config := NewTestConfig()
	config.Producer.Partitioner = func(string) sarama.Partitioner {
		return brokePartitioner{}
	}
	mp := NewAsyncProducer(trm, config)
	mp.ExpectInputWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		if msg.Partition != 15 {
			t.Error("Expected partition 15, found: ", msg.Partition)
		}
		if msg.Topic != "test" {
			t.Errorf(`Expected topic "test", found: %q`, msg.Topic)
		}
		return nil
	})
	mp.ExpectInputAndSucceed() // should actually fail in partitioning

	mp.Input() <- &sarama.ProducerMessage{Topic: "test"}
	mp.Input() <- &sarama.ProducerMessage{Topic: "not-test"}
	if err := mp.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 1 || !strings.Contains(trm.errors[0], "partitioning unavailable") {
		t.Error("Expected to report partitioning unavailable, found", trm.errors)
	}
}

// brokeProducer refuses to partition anything not on the “test” topic, and sends everything on
// that topic to partition 15.
type brokePartitioner struct{}

func (brokePartitioner) Partition(msg *sarama.ProducerMessage, n int32) (int32, error) {
	if msg.Topic == "test" {
		return 15, nil
	}
	return 0, errors.New("partitioning unavailable")
}

func (brokePartitioner) RequiresConsistency() bool { return false }

func TestProducerWithInvalidConfiguration(t *testing.T) {
	trm := newTestReporterMock()
	config := NewTestConfig()
	config.Version = sarama.V0_11_0_2
	config.ClientID = "not a valid producer ID"
	mp := NewAsyncProducer(trm, config)
	if err := mp.Close(); err != nil {
		t.Error(err)
	}
	if len(trm.errors) != 1 {
		t.Error("Expected to report a single error")
	} else if !strings.Contains(trm.errors[0], `ClientID value "not a valid producer ID" is not valid for Kafka versions before 1.0.0`) {
		t.Errorf("Unexpected error: %s", trm.errors[0])
	}
}

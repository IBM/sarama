package kafkamocks

import (
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
)

type testReporterMock struct {
	errors []string
}

func newTestReporterMock() *testReporterMock {
	return &testReporterMock{errors: make([]string, 0)}
}

func (trm *testReporterMock) Errorf(format string, args ...interface{}) {
	trm.errors = append(trm.errors, fmt.Sprintf(format, args...))
}

func TestMockProducerImplementsKafkaProducer(t *testing.T) {
	var mp interface{} = &MockProducer{}
	if _, ok := mp.(KafkaProducer); !ok {
		t.Error("MockProducer should implement the KafkaProducer interface.")
	}
}

func TestSaramaProducerImplementsKafkaProducer(t *testing.T) {
	var sp interface{} = &sarama.Producer{}
	if _, ok := sp.(KafkaProducer); !ok {
		t.Error("sarama.Producer should implement the KafkaProducer interface.")
	}
}

func TestReturnExpectationsToChannels(t *testing.T) {
	config := sarama.NewConfig()
	config.Producer.AckSuccesses = true
	mp := NewMockProducer(t, config)

	mp.ExpectInputAndSucceed()
	mp.ExpectInputAndSucceed()
	mp.ExpectInputAndFail(sarama.ErrOutOfBrokers)

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

	if err1.Msg.Topic != "test 3" || err1.Err != sarama.ErrOutOfBrokers {
		t.Error("Expected message 3 to be returned as error")
	}

	mp.Close()
}

func TestTooFewExpectations(t *testing.T) {
	trm := newTestReporterMock()
	mp := NewMockProducer(trm, nil)
	mp.ExpectInputAndSucceed()

	mp.Input() <- &sarama.ProducerMessage{Topic: "test"}
	mp.Input() <- &sarama.ProducerMessage{Topic: "test"}

	mp.Close()

	if len(trm.errors) != 1 {
		t.Error("Expected to report an error")
	}
}

func TestTooManyExpectations(t *testing.T) {
	trm := newTestReporterMock()
	mp := NewMockProducer(trm, nil)
	mp.ExpectInputAndSucceed()
	mp.ExpectInputAndFail(sarama.ErrOutOfBrokers)

	mp.Input() <- &sarama.ProducerMessage{Topic: "test"}
	mp.Close()

	if len(trm.errors) != 1 {
		t.Error("Expected to report an error")
	}
}

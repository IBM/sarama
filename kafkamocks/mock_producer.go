package kafkamocks

import (
	"errors"

	"github.com/Shopify/sarama"
)

type TestReporter interface {
	Errorf(string, ...interface{})
}

type KafkaProducer interface {
	AsyncClose()
	Close() error
	Input() chan<- *sarama.ProducerMessage
	Successes() <-chan *sarama.ProducerMessage
	Errors() <-chan *sarama.ProducerError
}

var (
	errProduceSuccess    error = nil
	errOutOfExpectations       = errors.New("No more expectations set on MockProducer")
)

type MockProducerExpectation struct {
	Result error
}

type MockProducer struct {
	expectations []*MockProducerExpectation
	closed       chan struct{}
	input        chan *sarama.ProducerMessage
	successes    chan *sarama.ProducerMessage
	errors       chan *sarama.ProducerError
}

func NewMockProducer(t TestReporter, config *sarama.Config) *MockProducer {
	if config == nil {
		config = sarama.NewConfig()
	}
	mp := &MockProducer{
		closed:       make(chan struct{}, 0),
		expectations: make([]*MockProducerExpectation, 0),
		input:        make(chan *sarama.ProducerMessage, config.ChannelBufferSize),
		successes:    make(chan *sarama.ProducerMessage, config.ChannelBufferSize),
		errors:       make(chan *sarama.ProducerError, config.ChannelBufferSize),
	}

	go func() {
		defer func() {
			close(mp.successes)
			close(mp.errors)
		}()

		for msg := range mp.input {
			if mp.expectations == nil || len(mp.expectations) == 0 {
				mp.expectations = nil
				t.Errorf("No more expectation set on this mock producer to handle the input message.")
			} else {
				expectation := mp.expectations[0]
				mp.expectations = mp.expectations[1:]
				if expectation.Result == errProduceSuccess && config.Producer.AckSuccesses {
					mp.successes <- msg
				} else {
					mp.errors <- &sarama.ProducerError{Err: expectation.Result, Msg: msg}
				}
			}
		}

		if len(mp.expectations) > 0 {
			t.Errorf("Expected to exhaust all expectations, but %d are left.", len(mp.expectations))
		}

		close(mp.closed)
	}()

	return mp
}

// Implement KafkaProducer interface

func (mp *MockProducer) AsyncClose() {
	close(mp.input)
}

func (mp *MockProducer) Close() error {
	mp.AsyncClose()
	<-mp.closed
	return nil
}

func (mp *MockProducer) Input() chan<- *sarama.ProducerMessage {
	return mp.input
}

func (mp *MockProducer) Successes() <-chan *sarama.ProducerMessage {
	return mp.successes
}

func (mp *MockProducer) Errors() <-chan *sarama.ProducerError {
	return mp.errors
}

// Setting expectations

func (mp *MockProducer) ExpectInputAndSucceed() {
	mp.expectations = append(mp.expectations, &MockProducerExpectation{Result: errProduceSuccess})
}

func (mp *MockProducer) ExpectInputAndFail(err error) {
	mp.expectations = append(mp.expectations, &MockProducerExpectation{Result: err})
}

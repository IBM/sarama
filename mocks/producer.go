package mocks

import (
	"errors"

	"github.com/Shopify/sarama"
)

type TestReporter interface {
	Errorf(string, ...interface{})
}

var (
	errProduceSuccess    error = nil
	errOutOfExpectations       = errors.New("No more expectations set on mock producer")
)

type producerExpectation struct {
	Result error
}

type Producer struct {
	expectations []*producerExpectation
	closed       chan struct{}
	input        chan *sarama.ProducerMessage
	successes    chan *sarama.ProducerMessage
	errors       chan *sarama.ProducerError
}

func NewProducer(t TestReporter, config *sarama.Config) *Producer {
	if config == nil {
		config = sarama.NewConfig()
	}
	mp := &Producer{
		closed:       make(chan struct{}, 0),
		expectations: make([]*producerExpectation, 0),
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

func (mp *Producer) AsyncClose() {
	close(mp.input)
}

func (mp *Producer) Close() error {
	mp.AsyncClose()
	<-mp.closed
	return nil
}

func (mp *Producer) Input() chan<- *sarama.ProducerMessage {
	return mp.input
}

func (mp *Producer) Successes() <-chan *sarama.ProducerMessage {
	return mp.successes
}

func (mp *Producer) Errors() <-chan *sarama.ProducerError {
	return mp.errors
}

// Setting expectations

func (mp *Producer) ExpectInputAndSucceed() {
	mp.expectations = append(mp.expectations, &producerExpectation{Result: errProduceSuccess})
}

func (mp *Producer) ExpectInputAndFail(err error) {
	mp.expectations = append(mp.expectations, &producerExpectation{Result: err})
}

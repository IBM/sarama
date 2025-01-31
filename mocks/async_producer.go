package mocks

import (
	"errors"
	"sync"

	"github.com/IBM/sarama"
)

// AsyncProducer implements sarama's Producer interface for testing purposes.
// Before you can send messages to it's Input channel, you have to set expectations
// so it knows how to handle the input; it returns an error if the number of messages
// received is bigger then the number of expectations set. You can also set a
// function in each expectation so that the message is checked by this function and
// an error is returned if the match fails.
type AsyncProducer struct {
	l               sync.Mutex
	t               ErrorReporter
	expectations    []*producerExpectation
	closed          chan struct{}
	input           chan *sarama.ProducerMessage
	successes       chan *sarama.ProducerMessage
	errors          chan *sarama.ProducerError
	isTransactional bool
	txnLock         sync.Mutex
	txnStatus       sarama.ProducerTxnStatusFlag
	lastOffset      int64
	*TopicConfig
}

// NewAsyncProducer instantiates a new Producer mock. The t argument should
// be the *testing.T instance of your test method. An error will be written to it if
// an expectation is violated. The config argument is validated and used to determine
// whether it should ack successes on the Successes channel and handle partitioning.
func NewAsyncProducer(t ErrorReporter, config *sarama.Config) *AsyncProducer {
	if config == nil {
		config = sarama.NewConfig()
	}
	if err := config.Validate(); err != nil {
		t.Errorf("Invalid mock configuration provided: %s", err.Error())
	}
	mp := &AsyncProducer{
		t:               t,
		closed:          make(chan struct{}),
		expectations:    make([]*producerExpectation, 0),
		input:           make(chan *sarama.ProducerMessage, config.ChannelBufferSize),
		successes:       make(chan *sarama.ProducerMessage, config.ChannelBufferSize),
		errors:          make(chan *sarama.ProducerError, config.ChannelBufferSize),
		isTransactional: config.Producer.Transaction.ID != "",
		txnStatus:       sarama.ProducerTxnFlagReady,
		TopicConfig:     NewTopicConfig(),
	}

	go func() {
		defer func() {
			close(mp.successes)
			close(mp.errors)
			close(mp.closed)
		}()

		partitioners := make(map[string]sarama.Partitioner, 1)

		for msg := range mp.input {
			mp.txnLock.Lock()
			if mp.IsTransactional() && mp.txnStatus&sarama.ProducerTxnFlagInTransaction == 0 {
				mp.t.Errorf("attempt to send message when transaction is not started or is in ending state.")
				mp.errors <- &sarama.ProducerError{Err: errors.New("attempt to send message when transaction is not started or is in ending state"), Msg: msg}
				continue
			}
			mp.txnLock.Unlock()
			partitioner := partitioners[msg.Topic]
			if partitioner == nil {
				partitioner = config.Producer.Partitioner(msg.Topic)
				partitioners[msg.Topic] = partitioner
			}
			mp.l.Lock()
			if len(mp.expectations) == 0 {
				mp.expectations = nil
				mp.t.Errorf("No more expectation set on this mock producer to handle the input message.")
			} else {
				expectation := mp.expectations[0]
				mp.expectations = mp.expectations[1:]

				partition, err := partitioner.Partition(msg, mp.partitions(msg.Topic))
				if err != nil {
					mp.t.Errorf("Partitioner returned an error: %s", err.Error())
					mp.errors <- &sarama.ProducerError{Err: err, Msg: msg}
				} else {
					msg.Partition = partition
					if expectation.CheckFunction != nil {
						err := expectation.CheckFunction(msg)
						if err != nil {
							mp.t.Errorf("Check function returned an error: %s", err.Error())
							mp.errors <- &sarama.ProducerError{Err: err, Msg: msg}
						}
					}
					if errors.Is(expectation.Result, errProduceSuccess) {
						mp.lastOffset++
						if config.Producer.Return.Successes {
							msg.Offset = mp.lastOffset
							mp.successes <- msg
						}
					} else if config.Producer.Return.Errors {
						mp.errors <- &sarama.ProducerError{Err: expectation.Result, Msg: msg}
					}
				}
			}
			mp.l.Unlock()
		}

		mp.l.Lock()
		if len(mp.expectations) > 0 {
			mp.t.Errorf("Expected to exhaust all expectations, but %d are left.", len(mp.expectations))
		}
		mp.l.Unlock()
	}()

	return mp
}

////////////////////////////////////////////////
// Implement Producer interface
////////////////////////////////////////////////

// AsyncClose corresponds with the AsyncClose method of sarama's Producer implementation.
// By closing a mock producer, you also tell it that no more input will be provided, so it will
// write an error to the test state if there's any remaining expectations.
func (mp *AsyncProducer) AsyncClose() {
	close(mp.input)
}

// Close corresponds with the Close method of sarama's Producer implementation.
// By closing a mock producer, you also tell it that no more input will be provided, so it will
// write an error to the test state if there's any remaining expectations.
func (mp *AsyncProducer) Close() error {
	mp.AsyncClose()
	<-mp.closed
	return nil
}

// Input corresponds with the Input method of sarama's Producer implementation.
// You have to set expectations on the mock producer before writing messages to the Input
// channel, so it knows how to handle them. If there is no more remaining expectations and
// a messages is written to the Input channel, the mock producer will write an error to the test
// state object.
func (mp *AsyncProducer) Input() chan<- *sarama.ProducerMessage {
	return mp.input
}

// Successes corresponds with the Successes method of sarama's Producer implementation.
func (mp *AsyncProducer) Successes() <-chan *sarama.ProducerMessage {
	return mp.successes
}

// Errors corresponds with the Errors method of sarama's Producer implementation.
func (mp *AsyncProducer) Errors() <-chan *sarama.ProducerError {
	return mp.errors
}

func (mp *AsyncProducer) IsTransactional() bool {
	return mp.isTransactional
}

func (mp *AsyncProducer) BeginTxn() error {
	mp.txnLock.Lock()
	defer mp.txnLock.Unlock()

	mp.txnStatus = sarama.ProducerTxnFlagInTransaction
	return nil
}

func (mp *AsyncProducer) CommitTxn() error {
	mp.txnLock.Lock()
	defer mp.txnLock.Unlock()

	mp.txnStatus = sarama.ProducerTxnFlagReady
	return nil
}

func (mp *AsyncProducer) AbortTxn() error {
	mp.txnLock.Lock()
	defer mp.txnLock.Unlock()

	mp.txnStatus = sarama.ProducerTxnFlagReady
	return nil
}

func (mp *AsyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	mp.txnLock.Lock()
	defer mp.txnLock.Unlock()

	return mp.txnStatus
}

func (mp *AsyncProducer) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupId string) error {
	return nil
}

func (mp *AsyncProducer) AddMessageToTxn(msg *sarama.ConsumerMessage, groupId string, metadata *string) error {
	return nil
}

////////////////////////////////////////////////
// Setting expectations
////////////////////////////////////////////////

// ExpectInputWithMessageCheckerFunctionAndSucceed sets an expectation on the mock producer that a
// message will be provided on the input channel. The mock producer will call the given function to
// check the message. If an error is returned it will be made available on the Errors channel
// otherwise the mock will handle the message as if it produced successfully, i.e. it will make it
// available on the Successes channel if the Producer.Return.Successes setting is set to true.
func (mp *AsyncProducer) ExpectInputWithMessageCheckerFunctionAndSucceed(cf MessageChecker) *AsyncProducer {
	mp.l.Lock()
	defer mp.l.Unlock()
	mp.expectations = append(mp.expectations, &producerExpectation{Result: errProduceSuccess, CheckFunction: cf})

	return mp
}

// ExpectInputWithMessageCheckerFunctionAndFail sets an expectation on the mock producer that a
// message will be provided on the input channel. The mock producer will first call the given
// function to check the message. If an error is returned it will be made available on the Errors
// channel otherwise the mock will handle the message as if it failed to produce successfully. This
// means it will make a ProducerError available on the Errors channel.
func (mp *AsyncProducer) ExpectInputWithMessageCheckerFunctionAndFail(cf MessageChecker, err error) *AsyncProducer {
	mp.l.Lock()
	defer mp.l.Unlock()
	mp.expectations = append(mp.expectations, &producerExpectation{Result: err, CheckFunction: cf})

	return mp
}

// ExpectInputWithCheckerFunctionAndSucceed sets an expectation on the mock producer that a message
// will be provided on the input channel. The mock producer will call the given function to check
// the message value. If an error is returned it will be made available on the Errors channel
// otherwise the mock will handle the message as if it produced successfully, i.e. it will make
// it available on the Successes channel if the Producer.Return.Successes setting is set to true.
func (mp *AsyncProducer) ExpectInputWithCheckerFunctionAndSucceed(cf ValueChecker) *AsyncProducer {
	mp.ExpectInputWithMessageCheckerFunctionAndSucceed(messageValueChecker(cf))

	return mp
}

// ExpectInputWithCheckerFunctionAndFail sets an expectation on the mock producer that a message
// will be provided on the input channel. The mock producer will first call the given function to
// check the message value. If an error is returned it will be made available on the Errors channel
// otherwise the mock will handle the message as if it failed to produce successfully. This means
// it will make a ProducerError available on the Errors channel.
func (mp *AsyncProducer) ExpectInputWithCheckerFunctionAndFail(cf ValueChecker, err error) *AsyncProducer {
	mp.ExpectInputWithMessageCheckerFunctionAndFail(messageValueChecker(cf), err)

	return mp
}

// ExpectInputAndSucceed sets an expectation on the mock producer that a message will be provided
// on the input channel. The mock producer will handle the message as if it is produced successfully,
// i.e. it will make it available on the Successes channel if the Producer.Return.Successes setting
// is set to true.
func (mp *AsyncProducer) ExpectInputAndSucceed() *AsyncProducer {
	mp.ExpectInputWithMessageCheckerFunctionAndSucceed(nil)

	return mp
}

// ExpectInputAndFail sets an expectation on the mock producer that a message will be provided
// on the input channel. The mock producer will handle the message as if it failed to produce
// successfully. This means it will make a ProducerError available on the Errors channel.
func (mp *AsyncProducer) ExpectInputAndFail(err error) *AsyncProducer {
	mp.ExpectInputWithMessageCheckerFunctionAndFail(nil, err)

	return mp
}

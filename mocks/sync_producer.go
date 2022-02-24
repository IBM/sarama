package mocks

import (
	"errors"
	"sync"

	"github.com/Shopify/sarama"
)

// SyncProducer implements sarama's SyncProducer interface for testing purposes.
// Before you can use it, you have to set expectations on the mock SyncProducer
// to tell it how to handle calls to SendMessage, so you can easily test success
// and failure scenarios.
type SyncProducer struct {
	l            sync.Mutex
	t            ErrorReporter
	expectations []*producerExpectation
	lastOffset   int64

	*TopicConfig
	newPartitioner sarama.PartitionerConstructor
	partitioners   map[string]sarama.Partitioner
}

// NewSyncProducer instantiates a new SyncProducer mock. The t argument should
// be the *testing.T instance of your test method. An error will be written to it if
// an expectation is violated. The config argument is used to handle partitioning.
func NewSyncProducer(t ErrorReporter, config *sarama.Config) *SyncProducer {
	if config == nil {
		config = sarama.NewConfig()
	}
	return &SyncProducer{
		t:              t,
		expectations:   make([]*producerExpectation, 0),
		TopicConfig:    NewTopicConfig(),
		newPartitioner: config.Producer.Partitioner,
		partitioners:   make(map[string]sarama.Partitioner, 1),
	}
}

////////////////////////////////////////////////
// Implement SyncProducer interface
////////////////////////////////////////////////

// SendMessage corresponds with the SendMessage method of sarama's SyncProducer implementation.
// You have to set expectations on the mock producer before calling SendMessage, so it knows
// how to handle them. You can set a function in each expectation so that the message value
// checked by this function and an error is returned if the match fails.
// If there is no more remaining expectation when SendMessage is called,
// the mock producer will write an error to the test state object.
func (sp *SyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	sp.l.Lock()
	defer sp.l.Unlock()

	if len(sp.expectations) > 0 {
		expectation := sp.expectations[0]
		sp.expectations = sp.expectations[1:]
		topic := msg.Topic
		partition, err := sp.partitioner(topic).Partition(msg, sp.partitions(topic))
		if err != nil {
			sp.t.Errorf("Partitioner returned an error: %s", err.Error())
			return -1, -1, err
		}
		msg.Partition = partition
		if expectation.CheckFunction != nil {
			errCheck := expectation.CheckFunction(msg)
			if errCheck != nil {
				sp.t.Errorf("Check function returned an error: %s", errCheck.Error())
				return -1, -1, errCheck
			}
		}
		if errors.Is(expectation.Result, errProduceSuccess) {
			sp.lastOffset++
			msg.Offset = sp.lastOffset
			return 0, msg.Offset, nil
		}
		return -1, -1, expectation.Result
	}
	sp.t.Errorf("No more expectation set on this mock producer to handle the input message.")
	return -1, -1, errOutOfExpectations
}

// SendMessages corresponds with the SendMessages method of sarama's SyncProducer implementation.
// You have to set expectations on the mock producer before calling SendMessages, so it knows
// how to handle them. If there is no more remaining expectations when SendMessages is called,
// the mock producer will write an error to the test state object.
func (sp *SyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	sp.l.Lock()
	defer sp.l.Unlock()

	if len(sp.expectations) >= len(msgs) {
		expectations := sp.expectations[0:len(msgs)]
		sp.expectations = sp.expectations[len(msgs):]

		for i, expectation := range expectations {
			topic := msgs[i].Topic
			partition, err := sp.partitioner(topic).Partition(msgs[i], sp.partitions(topic))
			if err != nil {
				sp.t.Errorf("Partitioner returned an error: %s", err.Error())
				return err
			}
			msgs[i].Partition = partition
			if expectation.CheckFunction != nil {
				errCheck := expectation.CheckFunction(msgs[i])
				if errCheck != nil {
					sp.t.Errorf("Check function returned an error: %s", errCheck.Error())
					return errCheck
				}
			}
			if !errors.Is(expectation.Result, errProduceSuccess) {
				return expectation.Result
			}
			sp.lastOffset++
			msgs[i].Offset = sp.lastOffset
		}
		return nil
	}
	sp.t.Errorf("Insufficient expectations set on this mock producer to handle the input messages.")
	return errOutOfExpectations
}

func (sp *SyncProducer) partitioner(topic string) sarama.Partitioner {
	partitioner := sp.partitioners[topic]
	if partitioner == nil {
		partitioner = sp.newPartitioner(topic)
		sp.partitioners[topic] = partitioner
	}
	return partitioner
}

// Close corresponds with the Close method of sarama's SyncProducer implementation.
// By closing a mock syncproducer, you also tell it that no more SendMessage calls will follow,
// so it will write an error to the test state if there's any remaining expectations.
func (sp *SyncProducer) Close() error {
	sp.l.Lock()
	defer sp.l.Unlock()

	if len(sp.expectations) > 0 {
		sp.t.Errorf("Expected to exhaust all expectations, but %d are left.", len(sp.expectations))
	}

	return nil
}

////////////////////////////////////////////////
// Setting expectations
////////////////////////////////////////////////

// ExpectSendMessageWithMessageCheckerFunctionAndSucceed sets an expectation on the mock producer
// that SendMessage will be called. The mock producer will first call the given function to check
// the message. It will cascade the error of the function, if any, or handle the message as if it
// produced successfully, i.e. by returning a valid partition, and offset, and a nil error.
func (sp *SyncProducer) ExpectSendMessageWithMessageCheckerFunctionAndSucceed(cf MessageChecker) *SyncProducer {
	sp.l.Lock()
	defer sp.l.Unlock()
	sp.expectations = append(sp.expectations, &producerExpectation{Result: errProduceSuccess, CheckFunction: cf})

	return sp
}

// ExpectSendMessageWithMessageCheckerFunctionAndFail sets an expectation on the mock producer that
// SendMessage will be called. The mock producer will first call the given function to check the
// message. It will cascade the error of the function, if any, or handle the message as if it
// failed to produce successfully, i.e. by returning the provided error.
func (sp *SyncProducer) ExpectSendMessageWithMessageCheckerFunctionAndFail(cf MessageChecker, err error) *SyncProducer {
	sp.l.Lock()
	defer sp.l.Unlock()
	sp.expectations = append(sp.expectations, &producerExpectation{Result: err, CheckFunction: cf})

	return sp
}

// ExpectSendMessageWithCheckerFunctionAndSucceed sets an expectation on the mock producer that SendMessage
// will be called. The mock producer will first call the given function to check the message value.
// It will cascade the error of the function, if any, or handle the message as if it produced
// successfully, i.e. by returning a valid partition, and offset, and a nil error.
func (sp *SyncProducer) ExpectSendMessageWithCheckerFunctionAndSucceed(cf ValueChecker) *SyncProducer {
	sp.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(messageValueChecker(cf))

	return sp
}

// ExpectSendMessageWithCheckerFunctionAndFail sets an expectation on the mock producer that SendMessage will be
// called. The mock producer will first call the given function to check the message value.
// It will cascade the error of the function, if any, or handle the message as if it failed
// to produce successfully, i.e. by returning the provided error.
func (sp *SyncProducer) ExpectSendMessageWithCheckerFunctionAndFail(cf ValueChecker, err error) *SyncProducer {
	sp.ExpectSendMessageWithMessageCheckerFunctionAndFail(messageValueChecker(cf), err)

	return sp
}

// ExpectSendMessageAndSucceed sets an expectation on the mock producer that SendMessage will be
// called. The mock producer will handle the message as if it produced successfully, i.e. by
// returning a valid partition, and offset, and a nil error.
func (sp *SyncProducer) ExpectSendMessageAndSucceed() *SyncProducer {
	sp.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(nil)

	return sp
}

// ExpectSendMessageAndFail sets an expectation on the mock producer that SendMessage will be
// called. The mock producer will handle the message as if it failed to produce
// successfully, i.e. by returning the provided error.
func (sp *SyncProducer) ExpectSendMessageAndFail(err error) *SyncProducer {
	sp.ExpectSendMessageWithMessageCheckerFunctionAndFail(nil, err)

	return sp
}

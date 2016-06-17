package mocks

import (
	"regexp"
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
}

// NewSyncProducer instantiates a new SyncProducer mock. The t argument should
// be the *testing.T instance of your test method. An error will be written to it if
// an expectation is violated. The config argument is currently unused, but is
// maintained to be compatible with the async Producer.
func NewSyncProducer(t ErrorReporter, config *sarama.Config) *SyncProducer {
	return &SyncProducer{
		t:            t,
		expectations: make([]*producerExpectation, 0),
	}
}

////////////////////////////////////////////////
// Implement SyncProducer interface
////////////////////////////////////////////////

// SendMessage corresponds with the SendMessage method of sarama's SyncProducer implementation.
// You have to set expectations on the mock producer before calling SendMessage, so it knows
// how to handle them. You can set a regexp in each expectation so that the message value
// is matched against this regexp and an error is returned if the match fails.
// If there is no more remaining expectation when SendMessage is called,
// the mock producer will write an error to the test state object.
func (sp *SyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	sp.l.Lock()
	defer sp.l.Unlock()

	if len(sp.expectations) > 0 {
		expectation := sp.expectations[0]
		sp.expectations = sp.expectations[1:]
		if len(expectation.MatchPattern) != 0 {
			matched, err := regexp.MatchString(expectation.MatchPattern, msg.Value.String())
			if err != nil {
				sp.t.Errorf("Error while trying to match the input message with the expected pattern: " + err.Error())
				panic(err.Error())
			}
			if !matched {
				sp.t.Errorf("Input value \"%s\" did not match expected pattern \"%s\"", msg.Value.String(), expectation.MatchPattern)
				return -1, -1, errNoMatch
			}
		}
		if expectation.Result == errProduceSuccess {
			sp.lastOffset++
			msg.Offset = sp.lastOffset
			return 0, msg.Offset, nil
		} else {
			return -1, -1, expectation.Result
		}
	} else {
		sp.t.Errorf("No more expectation set on this mock producer to handle the input message.")
		return -1, -1, errOutOfExpectations
	}
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

// ExpectSendMessageWithPatternAndSucceed sets an expectation on the mock producer that SendMessage
// will be called with a message value matching a given regexp. The mock producer will first check the
// message value against the pattern. It will return an error if the matching fails or handle
// the message as if it produced successfully, i.e. by returning a valid partition, and offset, and a nil error.
func (sp *SyncProducer) ExpectSendMessageWithPatternAndSucceed(pattern string) {
	sp.l.Lock()
	defer sp.l.Unlock()
	sp.expectations = append(sp.expectations, &producerExpectation{Result: errProduceSuccess, MatchPattern: pattern})
}

// ExpectSendMessageAndFail sets an expectation on the mock producer that SendMessage will be
// called with a message value matching a given regexp. The mock producer will first check the
// message value against the pattern. It will return an error if the matching fails or handle
// the message as if it failed to produce successfully, i.e. by returning the provided error.
func (sp *SyncProducer) ExpectSendMessageWithPatternAndFail(pattern string, err error) {
	sp.l.Lock()
	defer sp.l.Unlock()
	sp.expectations = append(sp.expectations, &producerExpectation{Result: err, MatchPattern: pattern})
}

// ExpectSendMessageAndSucceed sets an expectation on the mock producer that SendMessage will be
// called. The mock producer will handle the message as if it produced successfully, i.e. by
// returning a valid partition, and offset, and a nil error.
func (sp *SyncProducer) ExpectSendMessageAndSucceed() {
	sp.ExpectSendMessageWithPatternAndSucceed("")
}

// ExpectSendMessageAndFail sets an expectation on the mock producer that SendMessage will be
// called. The mock producer will handle the message as if it failed to produce
// successfully, i.e. by returning the provided error.
func (sp *SyncProducer) ExpectSendMessageAndFail(err error) {
	sp.ExpectSendMessageWithPatternAndFail("", err)
}

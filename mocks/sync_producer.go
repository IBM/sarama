package mocks

import (
	"github.com/Shopify/sarama"
	"sync"
)

type SyncProducer struct {
	l            sync.Mutex
	t            TestReporter
	expectations []*producerExpectation
	lastOffset   int64
}

func NewSyncProducer(t TestReporter, config *sarama.Config) *SyncProducer {
	return &SyncProducer{
		t:            t,
		expectations: make([]*producerExpectation, 0),
	}
}

func (sp *SyncProducer) SendMessage(topic string, key, value sarama.Encoder) (partition int32, offset int64, err error) {
	sp.l.Lock()
	defer sp.l.Unlock()

	if len(sp.expectations) > 0 {
		expectation := sp.expectations[0]
		sp.expectations = sp.expectations[1:]

		if expectation.Result == errProduceSuccess {
			sp.lastOffset++
			return 0, sp.lastOffset, nil
		} else {
			return -1, -1, expectation.Result
		}
	} else {
		sp.t.Errorf("No more expectation set on this mock producer to handle the input message.")
		return -1, -1, errOutOfExpectations
	}
}

func (sp *SyncProducer) Close() error {
	sp.l.Lock()
	defer sp.l.Unlock()

	if len(sp.expectations) > 0 {
		sp.t.Errorf("Expected to exhaust all expectations, but %d are left.", len(sp.expectations))
	}

	return nil
}

func (sp *SyncProducer) ExpectSendMessageAndSucceed() {
	sp.l.Lock()
	defer sp.l.Unlock()
	sp.expectations = append(sp.expectations, &producerExpectation{Result: errProduceSuccess})
}

func (sp *SyncProducer) ExpectSendMessageAndFail(err error) {
	sp.l.Lock()
	defer sp.l.Unlock()
	sp.expectations = append(sp.expectations, &producerExpectation{Result: err})
}

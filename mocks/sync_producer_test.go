package mocks

import (
	"testing"

	"github.com/Shopify/sarama"
)

func TestMockSyncProducerImplementsSyncProducerInterface(t *testing.T) {
	var mp interface{} = &SyncProducer{}
	if _, ok := mp.(sarama.SyncProducer); !ok {
		t.Error("The mock async producer should implement the sarama.SyncProducer interface.")
	}
}

func TestSyncProducerReturnsExpectationsToSendMessage(t *testing.T) {
	sp := NewSyncProducer(t, nil)
	defer sp.Close()

	sp.ExpectSendMessageAndSucceed()
	sp.ExpectSendMessageAndSucceed()
	sp.ExpectSendMessageAndFail(sarama.ErrOutOfBrokers)

	var (
		offset int64
		err    error
	)

	_, offset, err = sp.SendMessage("test", nil, sarama.StringEncoder("test"))
	if err != nil {
		t.Errorf("The first message should have been produced successfully, but got %s", err)
	}
	if offset != 1 {
		t.Errorf("The first message should have been assigned offset 1, but got %d", offset)
	}

	_, offset, err = sp.SendMessage("test", nil, sarama.StringEncoder("test"))
	if err != nil {
		t.Errorf("The second message should have been produced successfully, but got %s", err)
	}
	if offset != 2 {
		t.Errorf("The second message should have been assigned offset 2, but got %d", offset)
	}

	_, offset, err = sp.SendMessage("test", nil, sarama.StringEncoder("test"))
	if err != sarama.ErrOutOfBrokers {
		t.Errorf("The third message should not have been produced successfully")
	}

	sp.Close()
}

func TestSyncProducerWithTooManyExpectations(t *testing.T) {
	trm := newTestReporterMock()

	sp := NewSyncProducer(trm, nil)
	sp.ExpectSendMessageAndSucceed()
	sp.ExpectSendMessageAndFail(sarama.ErrOutOfBrokers)

	sp.SendMessage("test", nil, sarama.StringEncoder("test"))

	sp.Close()

	if len(trm.errors) != 1 {
		t.Error("Expected to report an error")
	}
}

func TestSyncProducerWithTooFewExpectations(t *testing.T) {
	trm := newTestReporterMock()

	sp := NewSyncProducer(trm, nil)
	sp.ExpectSendMessageAndSucceed()

	sp.SendMessage("test", nil, sarama.StringEncoder("test"))
	sp.SendMessage("test", nil, sarama.StringEncoder("test"))

	sp.Close()

	if len(trm.errors) != 1 {
		t.Error("Expected to report an error")
	}
}

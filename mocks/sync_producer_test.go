//go:build !functional

package mocks

import (
	"errors"
	"strings"
	"testing"

	"github.com/IBM/sarama"
)

func TestMockSyncProducerImplementsSyncProducerInterface(t *testing.T) {
	var mp interface{} = &SyncProducer{}
	if _, ok := mp.(sarama.SyncProducer); !ok {
		t.Error("The mock async producer should implement the sarama.SyncProducer interface.")
	}
}

func TestSyncProducerReturnsExpectationsToSendMessage(t *testing.T) {
	sp := NewSyncProducer(t, nil)
	defer func() {
		if err := sp.Close(); err != nil {
			t.Error(err)
		}
	}()

	sp.ExpectSendMessageAndSucceed()
	sp.ExpectSendMessageAndSucceed()
	sp.ExpectSendMessageAndFail(sarama.ErrOutOfBrokers)

	msg := &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("test")}

	_, offset, err := sp.SendMessage(msg)
	if err != nil {
		t.Errorf("The first message should have been produced successfully, but got %s", err)
	}
	if offset != 1 || offset != msg.Offset {
		t.Errorf("The first message should have been assigned offset 1, but got %d", msg.Offset)
	}

	_, offset, err = sp.SendMessage(msg)
	if err != nil {
		t.Errorf("The second message should have been produced successfully, but got %s", err)
	}
	if offset != 2 || offset != msg.Offset {
		t.Errorf("The second message should have been assigned offset 2, but got %d", offset)
	}

	_, _, err = sp.SendMessage(msg)
	if !errors.Is(err, sarama.ErrOutOfBrokers) {
		t.Errorf("The third message should not have been produced successfully")
	}

	if err := sp.Close(); err != nil {
		t.Error(err)
	}
}

func TestSyncProducerFailTxn(t *testing.T) {
	config := NewTestConfig()
	config.Producer.Transaction.ID = "test"
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Backoff = 0
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
	config.Version = sarama.V0_11_0_0

	tfm := newTestReporterMock()

	sp := NewSyncProducer(tfm, config)
	defer func() {
		if err := sp.Close(); err != nil {
			t.Error(err)
		}
	}()

	msg := &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("test")}

	_, _, err := sp.SendMessage(msg)
	if err == nil {
		t.Errorf("must have failed with txn begin error")
	}

	if len(tfm.errors) != 1 {
		t.Errorf("must have failed with txn begin error")
	}
}

func TestSyncProducerUseTxn(t *testing.T) {
	config := NewTestConfig()
	config.Producer.Transaction.ID = "test"
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Backoff = 0
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
	config.Version = sarama.V0_11_0_0

	sp := NewSyncProducer(t, config)
	defer func() {
		if err := sp.Close(); err != nil {
			t.Error(err)
		}
	}()

	if !sp.IsTransactional() {
		t.Error("producer must be transactional")
	}

	sp.ExpectSendMessageAndSucceed()

	msg := &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("test")}

	err := sp.BeginTxn()
	if err != nil {
		t.Errorf("txn can't be started, got %s", err)
	}
	if sp.TxnStatus()&sarama.ProducerTxnFlagInTransaction == 0 {
		t.Error("transaction must be started")
	}
	_, offset, err := sp.SendMessage(msg)
	if err != nil {
		t.Errorf("The first message should have been produced successfully, but got %s", err)
	}
	if offset != 1 || offset != msg.Offset {
		t.Errorf("The first message should have been assigned offset 1, but got %d", msg.Offset)
	}

	if err := sp.AddMessageToTxn(&sarama.ConsumerMessage{
		Topic:     "original-topic",
		Partition: 0,
		Offset:    123,
	}, "test-group", nil); err != nil {
		t.Error(err)
	}

	if err := sp.AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata{
		"original-topic": {
			{
				Partition: 1,
				Offset:    321,
			},
		},
	}, "test-group"); err != nil {
		t.Error(err)
	}

	err = sp.CommitTxn()
	if err != nil {
		t.Errorf("txn can't be committed, got %s", err)
	}

	if err := sp.Close(); err != nil {
		t.Error(err)
	}
}

func TestSyncProducerWithTooManyExpectations(t *testing.T) {
	trm := newTestReporterMock()

	sp := NewSyncProducer(trm, nil).
		ExpectSendMessageAndSucceed().
		ExpectSendMessageAndFail(sarama.ErrOutOfBrokers)

	msg := &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("test")}
	if _, _, err := sp.SendMessage(msg); err != nil {
		t.Error("No error expected on first SendMessage call", err)
	}

	if err := sp.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 1 {
		t.Error("Expected to report an error")
	}
}

func TestSyncProducerWithTooFewExpectations(t *testing.T) {
	trm := newTestReporterMock()

	sp := NewSyncProducer(trm, nil).ExpectSendMessageAndSucceed()

	msg := &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("test")}
	if _, _, err := sp.SendMessage(msg); err != nil {
		t.Error("No error expected on first SendMessage call", err)
	}
	if _, _, err := sp.SendMessage(msg); !errors.Is(err, errOutOfExpectations) {
		t.Error("errOutOfExpectations expected on second SendMessage call, found:", err)
	}

	if err := sp.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 1 {
		t.Error("Expected to report an error")
	}
}

func TestSyncProducerWithCheckerFunction(t *testing.T) {
	trm := newTestReporterMock()

	sp := NewSyncProducer(trm, nil).
		ExpectSendMessageWithCheckerFunctionAndSucceed(generateRegexpChecker("^tes")).
		ExpectSendMessageWithCheckerFunctionAndSucceed(generateRegexpChecker("^tes$"))

	msg := &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("test")}
	if _, _, err := sp.SendMessage(msg); err != nil {
		t.Error("No error expected on first SendMessage call, found: ", err)
	}
	msg = &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("test")}
	if _, _, err := sp.SendMessage(msg); err == nil || !strings.HasPrefix(err.Error(), "No match") {
		t.Error("Error during value check expected on second SendMessage call, found:", err)
	}

	if err := sp.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 1 {
		t.Error("Expected to report an error")
	}
}

func TestSyncProducerWithCheckerFunctionForSendMessagesWithError(t *testing.T) {
	trm := newTestReporterMock()

	sp := NewSyncProducer(trm, nil).
		ExpectSendMessageWithCheckerFunctionAndSucceed(generateRegexpChecker("^tes")).
		ExpectSendMessageWithCheckerFunctionAndSucceed(generateRegexpChecker("^tes$"))

	msg1 := &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("test")}
	msg2 := &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("test")}
	msgs := []*sarama.ProducerMessage{msg1, msg2}

	if err := sp.SendMessages(msgs); err == nil || !strings.HasPrefix(err.Error(), "No match") {
		t.Error("Error during value check expected on second message, found: ", err)
	}

	if err := sp.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 1 {
		t.Error("Expected to report an error")
	}
}

func TestSyncProducerWithCheckerFunctionForSendMessagesWithoutError(t *testing.T) {
	trm := newTestReporterMock()

	sp := NewSyncProducer(trm, nil).
		ExpectSendMessageWithCheckerFunctionAndSucceed(generateRegexpChecker("^tes"))

	msg1 := &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("test")}
	msgs := []*sarama.ProducerMessage{msg1}

	if err := sp.SendMessages(msgs); err != nil {
		t.Error("No error expected on SendMessages call, found: ", err)
	}

	for i, msg := range msgs {
		offset := int64(i + 1)
		if offset != msg.Offset {
			t.Errorf("The message should have been assigned offset %d, but got %d", offset, msg.Offset)
		}
	}

	if err := sp.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 0 {
		t.Errorf("Expected to not report any errors, found: %v", trm.errors)
	}
}

func TestSyncProducerSendMessagesExpectationsMismatchTooFew(t *testing.T) {
	trm := newTestReporterMock()

	sp := NewSyncProducer(trm, nil).
		ExpectSendMessageWithCheckerFunctionAndSucceed(generateRegexpChecker("^tes"))

	msg1 := &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("test")}
	msg2 := &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("test")}

	msgs := []*sarama.ProducerMessage{msg1, msg2}

	if err := sp.SendMessages(msgs); err == nil {
		t.Error("Error during value check expected on second message, found: ", err)
	}

	if err := sp.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 2 {
		t.Error("Expected to report 2 errors")
	}
}

func TestSyncProducerSendMessagesExpectationsMismatchTooMany(t *testing.T) {
	trm := newTestReporterMock()

	sp := NewSyncProducer(trm, nil).
		ExpectSendMessageWithCheckerFunctionAndSucceed(generateRegexpChecker("^tes")).
		ExpectSendMessageWithCheckerFunctionAndSucceed(generateRegexpChecker("^tes"))

	msg1 := &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("test")}
	msgs := []*sarama.ProducerMessage{msg1}

	if err := sp.SendMessages(msgs); err != nil {
		t.Error("No error expected on SendMessages call, found: ", err)
	}

	if err := sp.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 1 {
		t.Error("Expected to report 1 errors")
	}
}

func TestSyncProducerSendMessagesFaultyEncoder(t *testing.T) {
	trm := newTestReporterMock()

	sp := NewSyncProducer(trm, nil).
		ExpectSendMessageWithCheckerFunctionAndSucceed(generateRegexpChecker("^tes"))

	msg1 := &sarama.ProducerMessage{Topic: "test", Value: faultyEncoder("123")}
	msgs := []*sarama.ProducerMessage{msg1}

	if err := sp.SendMessages(msgs); err == nil || !strings.Contains(err.Error(), "encode error") {
		t.Error("Encoding error expected, found: ", err)
	}

	if err := sp.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 1 {
		t.Error("Expected to report 1 errors")
	}
}

type faultyEncoder []byte

func (f faultyEncoder) Encode() ([]byte, error) {
	return nil, errors.New("encode error")
}

func (f faultyEncoder) Length() int {
	return len(f)
}

func TestSyncProducerInvalidConfiguration(t *testing.T) {
	trm := newTestReporterMock()
	config := NewTestConfig()
	config.Version = sarama.V0_11_0_2
	config.ClientID = "not a valid producer ID"
	mp := NewSyncProducer(trm, config)
	if err := mp.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 1 {
		t.Error("Expected to report a single error")
	} else if !strings.Contains(trm.errors[0], `ClientID value "not a valid producer ID" is not valid for Kafka versions before 1.0.0`) {
		t.Errorf("Unexpected error: %s", trm.errors[0])
	}
}

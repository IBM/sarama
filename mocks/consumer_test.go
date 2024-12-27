//go:build !functional

package mocks

import (
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/IBM/sarama"
)

func TestMockConsumerImplementsConsumerInterface(t *testing.T) {
	var c interface{} = &Consumer{}
	if _, ok := c.(sarama.Consumer); !ok {
		t.Error("The mock consumer should implement the sarama.Consumer interface.")
	}

	var pc interface{} = &PartitionConsumer{}
	if _, ok := pc.(sarama.PartitionConsumer); !ok {
		t.Error("The mock partitionconsumer should implement the sarama.PartitionConsumer interface.")
	}
}

func TestConsumerHandlesExpectations(t *testing.T) {
	consumer := NewConsumer(t, NewTestConfig())
	defer func() {
		if err := consumer.Close(); err != nil {
			t.Error(err)
		}
	}()

	consumer.ExpectConsumePartition("test", 0, sarama.OffsetOldest).YieldMessage(&sarama.ConsumerMessage{Value: []byte("hello world")})
	consumer.ExpectConsumePartition("test", 0, sarama.OffsetOldest).YieldError(sarama.ErrOutOfBrokers)
	consumer.ExpectConsumePartition("test", 1, sarama.OffsetOldest).YieldMessage(&sarama.ConsumerMessage{Value: []byte("hello world again")})
	consumer.ExpectConsumePartition("other", 0, AnyOffset).YieldMessage(&sarama.ConsumerMessage{Value: []byte("hello other")})

	pc_test0, err := consumer.ConsumePartition("test", 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}
	test0_msg := <-pc_test0.Messages()
	if test0_msg.Topic != "test" || test0_msg.Partition != 0 || string(test0_msg.Value) != "hello world" {
		t.Error("Message was not as expected:", test0_msg)
	}
	test0_err := <-pc_test0.Errors()
	if !errors.Is(test0_err, sarama.ErrOutOfBrokers) {
		t.Error("Expected sarama.ErrOutOfBrokers, found:", test0_err.Err)
	}

	pc_test1, err := consumer.ConsumePartition("test", 1, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}
	test1_msg := <-pc_test1.Messages()
	if test1_msg.Topic != "test" || test1_msg.Partition != 1 || string(test1_msg.Value) != "hello world again" {
		t.Error("Message was not as expected:", test1_msg)
	}

	pc_other0, err := consumer.ConsumePartition("other", 0, sarama.OffsetNewest)
	if err != nil {
		t.Fatal(err)
	}
	other0_msg := <-pc_other0.Messages()
	if other0_msg.Topic != "other" || other0_msg.Partition != 0 || string(other0_msg.Value) != "hello other" {
		t.Error("Message was not as expected:", other0_msg)
	}
}

func TestConsumerHandlesExpectationsPausingResuming(t *testing.T) {
	consumer := NewConsumer(t, NewTestConfig())
	defer func() {
		if err := consumer.Close(); err != nil {
			t.Error(err)
		}
	}()

	consumePartitionT0P0 := consumer.ExpectConsumePartition("test", 0, sarama.OffsetOldest)
	consumePartitionT0P1 := consumer.ExpectConsumePartition("test", 1, sarama.OffsetOldest)
	consumePartitionT1P0 := consumer.ExpectConsumePartition("other", 0, AnyOffset)

	consumePartitionT0P0.Pause()
	consumePartitionT0P0.YieldMessage(&sarama.ConsumerMessage{Value: []byte("hello world")})
	consumePartitionT0P0.YieldMessage(&sarama.ConsumerMessage{Value: []byte("hello world x")})
	consumePartitionT0P0.YieldError(sarama.ErrOutOfBrokers)

	consumePartitionT0P1.YieldMessage(&sarama.ConsumerMessage{Value: []byte("hello world again")})

	consumePartitionT1P0.YieldMessage(&sarama.ConsumerMessage{Value: []byte("hello other")})

	pc_test0, err := consumer.ConsumePartition("test", 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}
	if len(pc_test0.Messages()) > 0 {
		t.Error("Problem to pause consumption")
	}
	test0_err := <-pc_test0.Errors()
	if !errors.Is(test0_err, sarama.ErrOutOfBrokers) {
		t.Error("Expected sarama.ErrOutOfBrokers, found:", test0_err.Err)
	}

	if pc_test0.HighWaterMarkOffset() != 0 {
		t.Error("High water mark offset with value different from the expected: ", pc_test0.HighWaterMarkOffset())
	}

	pc_test1, err := consumer.ConsumePartition("test", 1, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}
	test1_msg := <-pc_test1.Messages()
	if test1_msg.Topic != "test" || test1_msg.Partition != 1 || string(test1_msg.Value) != "hello world again" {
		t.Error("Message was not as expected:", test1_msg)
	}

	if pc_test1.HighWaterMarkOffset() != 1 {
		t.Error("High water mark offset with value different from the expected: ", pc_test1.HighWaterMarkOffset())
	}

	pc_other0, err := consumer.ConsumePartition("other", 0, sarama.OffsetNewest)
	if err != nil {
		t.Fatal(err)
	}
	other0_msg := <-pc_other0.Messages()
	if other0_msg.Topic != "other" || other0_msg.Partition != 0 || string(other0_msg.Value) != "hello other" {
		t.Error("Message was not as expected:", other0_msg)
	}

	if pc_other0.HighWaterMarkOffset() != AnyOffset+1 {
		t.Error("High water mark offset with value different from the expected: ", pc_other0.HighWaterMarkOffset())
	}

	pc_test0.Resume()
	test0_msg1 := <-pc_test0.Messages()
	if test0_msg1.Topic != "test" || test0_msg1.Partition != 0 || string(test0_msg1.Value) != "hello world" || test0_msg1.Offset != 0 {
		t.Error("Message was not as expected:", test0_msg1)
	}

	test0_msg2 := <-pc_test0.Messages()
	if test0_msg2.Topic != "test" || test0_msg2.Partition != 0 || string(test0_msg2.Value) != "hello world x" || test0_msg2.Offset != 1 {
		t.Error("Message was not as expected:", test0_msg2)
	}

	if pc_test0.HighWaterMarkOffset() != 2 {
		t.Error("High water mark offset with value different from the expected: ", pc_test0.HighWaterMarkOffset())
	}
}

func TestConsumerReturnsNonconsumedErrorsOnClose(t *testing.T) {
	consumer := NewConsumer(t, NewTestConfig())
	consumer.ExpectConsumePartition("test", 0, sarama.OffsetOldest).YieldError(sarama.ErrOutOfBrokers)
	consumer.ExpectConsumePartition("test", 0, sarama.OffsetOldest).YieldError(sarama.ErrOutOfBrokers)

	pc, err := consumer.ConsumePartition("test", 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-pc.Messages():
		t.Error("Did not expect a message on the messages channel.")
	case err := <-pc.Errors():
		if !errors.Is(err, sarama.ErrOutOfBrokers) {
			t.Error("Expected sarama.ErrOutOfBrokers, found", err)
		}
	}

	var errs sarama.ConsumerErrors
	if !errors.As(pc.Close(), &errs) {
		t.Error("Expected Close to return ConsumerErrors")
	}
	if len(errs) != 1 && !errors.Is(errs[0], sarama.ErrOutOfBrokers) {
		t.Error("Expected Close to return the remaining sarama.ErrOutOfBrokers")
	}
}

func TestConsumerWithoutExpectationsOnPartition(t *testing.T) {
	trm := newTestReporterMock()
	consumer := NewConsumer(trm, NewTestConfig())

	_, err := consumer.ConsumePartition("test", 1, sarama.OffsetOldest)
	if !errors.Is(err, errOutOfExpectations) {
		t.Error("Expected ConsumePartition to return errOutOfExpectations")
	}

	if err := consumer.Close(); err != nil {
		t.Error("No error expected on close, but found:", err)
	}

	if len(trm.errors) != 1 {
		t.Errorf("Expected an expectation failure to be set on the error reporter.")
	}
}

func TestConsumerWithExpectationsOnUnconsumedPartition(t *testing.T) {
	trm := newTestReporterMock()
	consumer := NewConsumer(trm, NewTestConfig())
	consumer.ExpectConsumePartition("test", 0, sarama.OffsetOldest).YieldMessage(&sarama.ConsumerMessage{Value: []byte("hello world")})

	if err := consumer.Close(); err != nil {
		t.Error("No error expected on close, but found:", err)
	}

	if len(trm.errors) != 1 {
		t.Errorf("Expected an expectation failure to be set on the error reporter.")
	}
}

func TestConsumerWithWrongOffsetExpectation(t *testing.T) {
	trm := newTestReporterMock()
	consumer := NewConsumer(trm, NewTestConfig())
	consumer.ExpectConsumePartition("test", 0, sarama.OffsetOldest)

	_, err := consumer.ConsumePartition("test", 0, sarama.OffsetNewest)
	if err != nil {
		t.Error("Did not expect error, found:", err)
	}

	if len(trm.errors) != 1 {
		t.Errorf("Expected an expectation failure to be set on the error reporter.")
	}

	if err := consumer.Close(); err != nil {
		t.Error(err)
	}
}

func TestConsumerViolatesMessagesDrainedExpectation(t *testing.T) {
	trm := newTestReporterMock()
	consumer := NewConsumer(trm, NewTestConfig())
	consumer.ExpectConsumePartition("test", 0, sarama.OffsetOldest).
		YieldMessage(&sarama.ConsumerMessage{Value: []byte("hello")}).
		YieldMessage(&sarama.ConsumerMessage{Value: []byte("hello")}).
		ExpectMessagesDrainedOnClose()

	pc, err := consumer.ConsumePartition("test", 0, sarama.OffsetOldest)
	if err != nil {
		t.Error(err)
	}

	// consume first message, not second one
	<-pc.Messages()

	if err := consumer.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 1 {
		t.Errorf("Expected an expectation failure to be set on the error reporter.")
	}
}

func TestConsumerMeetsErrorsDrainedExpectation(t *testing.T) {
	trm := newTestReporterMock()
	consumer := NewConsumer(trm, NewTestConfig())

	consumer.ExpectConsumePartition("test", 0, sarama.OffsetOldest).
		YieldError(sarama.ErrInvalidMessage).
		YieldError(sarama.ErrInvalidMessage).
		ExpectErrorsDrainedOnClose()

	pc, err := consumer.ConsumePartition("test", 0, sarama.OffsetOldest)
	if err != nil {
		t.Error(err)
	}

	// consume first and second error,
	<-pc.Errors()
	<-pc.Errors()

	if err := consumer.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 0 {
		t.Errorf("Expected no expectation failures to be set on the error reporter.")
	}
}

func TestConsumerTopicMetadata(t *testing.T) {
	trm := newTestReporterMock()
	consumer := NewConsumer(trm, NewTestConfig())

	consumer.SetTopicMetadata(map[string][]int32{
		"test1": {0, 1, 2, 3},
		"test2": {0, 1, 2, 3, 4, 5, 6, 7},
	})

	topics, err := consumer.Topics()
	if err != nil {
		t.Error(t)
	}

	sortedTopics := sort.StringSlice(topics)
	sortedTopics.Sort()
	if len(sortedTopics) != 2 || sortedTopics[0] != "test1" || sortedTopics[1] != "test2" {
		t.Error("Unexpected topics returned:", sortedTopics)
	}

	partitions1, err := consumer.Partitions("test1")
	if err != nil {
		t.Error(t)
	}

	if len(partitions1) != 4 {
		t.Error("Unexpected partitions returned:", len(partitions1))
	}

	partitions2, err := consumer.Partitions("test2")
	if err != nil {
		t.Error(t)
	}

	if len(partitions2) != 8 {
		t.Error("Unexpected partitions returned:", len(partitions2))
	}

	if len(trm.errors) != 0 {
		t.Errorf("Expected no expectation failures to be set on the error reporter.")
	}
}

func TestConsumerUnexpectedTopicMetadata(t *testing.T) {
	trm := newTestReporterMock()
	consumer := NewConsumer(trm, NewTestConfig())

	if _, err := consumer.Topics(); !errors.Is(err, sarama.ErrOutOfBrokers) {
		t.Error("Expected sarama.ErrOutOfBrokers, found", err)
	}

	if len(trm.errors) != 1 {
		t.Errorf("Expected an expectation failure to be set on the error reporter.")
	}
}

func TestConsumerOffsetsAreManagedCorrectlyWithOffsetOldest(t *testing.T) {
	trm := newTestReporterMock()
	consumer := NewConsumer(trm, NewTestConfig())
	pcmock := consumer.ExpectConsumePartition("test", 0, sarama.OffsetOldest)
	pcmock.YieldMessage(&sarama.ConsumerMessage{Value: []byte("hello")})
	pcmock.YieldMessage(&sarama.ConsumerMessage{Value: []byte("hello")})
	pcmock.ExpectMessagesDrainedOnClose()

	pc, err := consumer.ConsumePartition("test", 0, sarama.OffsetOldest)
	if err != nil {
		t.Error(err)
	}

	message1 := <-pc.Messages()
	if message1.Offset != 0 {
		t.Errorf("Expected offset of first message in the partition to be 0, got %d", message1.Offset)
	}

	message2 := <-pc.Messages()
	if message2.Offset != 1 {
		t.Errorf("Expected offset of second message in the partition to be 1, got %d", message2.Offset)
	}

	if err := consumer.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 0 {
		t.Errorf("Expected to not report any errors, found: %v", trm.errors)
	}
}

func TestConsumerOffsetsAreManagedCorrectlyWithSpecifiedOffset(t *testing.T) {
	startingOffset := int64(123)
	trm := newTestReporterMock()
	consumer := NewConsumer(trm, NewTestConfig())
	pcmock := consumer.ExpectConsumePartition("test", 0, startingOffset)
	pcmock.YieldMessage(&sarama.ConsumerMessage{Value: []byte("hello")})
	pcmock.YieldMessage(&sarama.ConsumerMessage{Value: []byte("hello")})
	pcmock.ExpectMessagesDrainedOnClose()

	pc, err := consumer.ConsumePartition("test", 0, startingOffset)
	if err != nil {
		t.Error(err)
	}

	message1 := <-pc.Messages()
	if message1.Offset != startingOffset {
		t.Errorf("Expected offset of first message to be %d, got %d", startingOffset, message1.Offset)
	}

	message2 := <-pc.Messages()
	if message2.Offset != startingOffset+1 {
		t.Errorf("Expected offset of second message to be %d, got %d", startingOffset+1, message2.Offset)
	}

	if err := consumer.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 0 {
		t.Errorf("Expected to not report any errors, found: %v", trm.errors)
	}

	if pc.HighWaterMarkOffset() != message2.Offset+1 {
		diff := pc.HighWaterMarkOffset() - message2.Offset
		t.Errorf("Difference between highwatermarkoffset and last message offset greater than 1, got: %v", diff)
	}
}

func TestConsumerInvalidConfiguration(t *testing.T) {
	trm := newTestReporterMock()
	config := NewTestConfig()
	config.Version = sarama.V0_11_0_2
	config.ClientID = "not a valid consumer ID"
	consumer := NewConsumer(trm, config)
	if err := consumer.Close(); err != nil {
		t.Error(err)
	}

	if len(trm.errors) != 1 {
		t.Error("Expected to report a single error")
	} else if !strings.Contains(trm.errors[0], `ClientID value "not a valid consumer ID" is not valid for Kafka versions before 1.0.0`) {
		t.Errorf("Unexpected error: %s", trm.errors[0])
	}
}

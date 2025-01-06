//go:build functional

package sarama

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/require"

	"github.com/IBM/sarama/internal/toxiproxy"
)

const TestBatchSize = 1000

func TestFuncProducing(t *testing.T) {
	config := NewFunctionalTestConfig()
	testProducingMessages(t, config, MinVersion)
}

func TestFuncProducingGzip(t *testing.T) {
	config := NewFunctionalTestConfig()
	config.Producer.Compression = CompressionGZIP
	testProducingMessages(t, config, MinVersion)
}

func TestFuncProducingSnappy(t *testing.T) {
	config := NewFunctionalTestConfig()
	config.Producer.Compression = CompressionSnappy
	testProducingMessages(t, config, MinVersion)
}

func TestFuncProducingZstd(t *testing.T) {
	config := NewFunctionalTestConfig()
	config.Producer.Compression = CompressionZSTD
	testProducingMessages(t, config, V2_1_0_0) // must be at least 2.1.0.0 for zstd
}

func TestFuncProducingNoResponse(t *testing.T) {
	config := NewFunctionalTestConfig()
	config.ApiVersionsRequest = false
	config.Producer.RequiredAcks = NoResponse
	testProducingMessages(t, config, MinVersion)
}

func TestFuncProducingFlushing(t *testing.T) {
	config := NewFunctionalTestConfig()
	config.Producer.Flush.Messages = TestBatchSize / 8
	config.Producer.Flush.Frequency = 250 * time.Millisecond
	testProducingMessages(t, config, MinVersion)
}

func TestFuncMultiPartitionProduce(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.ChannelBufferSize = 20
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.Flush.Messages = 200
	config.Producer.Return.Successes = true
	producer, err := NewSyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(TestBatchSize)

	for i := 1; i <= TestBatchSize; i++ {
		go func(i int) {
			defer wg.Done()
			msg := &ProducerMessage{Topic: "test.64", Key: nil, Value: StringEncoder(fmt.Sprintf("hur %d", i))}
			if _, _, err := producer.SendMessage(msg); err != nil {
				t.Error(i, err)
			}
		}(i)
	}

	wg.Wait()
	if err := producer.Close(); err != nil {
		t.Error(err)
	}
}

func TestFuncTxnProduceNoBegin(t *testing.T) {
	checkKafkaVersion(t, "0.11.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.ChannelBufferSize = 20
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.Flush.Messages = 200
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "TestFuncTxnProduceNoBegin"
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Retry.Max = 50
	config.Consumer.IsolationLevel = ReadCommitted
	config.Producer.Return.Errors = true
	config.Producer.Transaction.Retry.Max = 200
	config.Net.MaxOpenRequests = 1
	producer, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer producer.Close()

	producer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder("test")}
	producerError := <-producer.Errors()
	require.Error(t, producerError)
}

func TestFuncTxnCommitNoMessages(t *testing.T) {
	checkKafkaVersion(t, "0.11.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.ChannelBufferSize = 20
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.Flush.Messages = 200
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "TestFuncTxnCommitNoMessages"
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Retry.Max = 50
	config.Consumer.IsolationLevel = ReadCommitted
	config.Producer.Return.Errors = true
	config.Producer.Transaction.Retry.Max = 200
	config.Net.MaxOpenRequests = 1
	producer, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer producer.Close()

	err = producer.BeginTxn()
	require.NoError(t, err)

	err = producer.AbortTxn()
	require.NoError(t, err)

	err = producer.BeginTxn()
	require.NoError(t, err)

	err = producer.CommitTxn()
	require.NoError(t, err)
}

func TestFuncTxnProduce(t *testing.T) {
	checkKafkaVersion(t, "0.11.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.ChannelBufferSize = 20
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.Flush.Messages = 200
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "TestFuncTxnProduce"
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Transaction.Retry.Max = 200
	config.Consumer.IsolationLevel = ReadCommitted
	config.Net.MaxOpenRequests = 1

	consumer, err := NewConsumer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer consumer.Close()

	pc, err := consumer.ConsumePartition("test.1", 0, OffsetNewest)
	msgChannel := pc.Messages()
	require.NoError(t, err)
	defer pc.Close()

	nonTransactionalProducer, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, NewFunctionalTestConfig())
	require.NoError(t, err)
	defer nonTransactionalProducer.Close()

	// Ensure consumer is started
	nonTransactionalProducer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder("test")}
	<-msgChannel

	producer, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer producer.Close()

	err = producer.BeginTxn()
	require.NoError(t, err)

	for i := 0; i < 1; i++ {
		producer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder("test")}
	}

	err = producer.CommitTxn()
	require.NoError(t, err)

	for i := 0; i < 1; i++ {
		msg := <-msgChannel
		t.Logf("Received %s from %s-%d at offset %d", msg.Value, msg.Topic, msg.Partition, msg.Offset)
	}
}

func TestFuncTxnProduceWithBrokerFailure(t *testing.T) {
	checkKafkaVersion(t, "0.11.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.ChannelBufferSize = 20
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.Flush.Messages = 200
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "TestFuncTxnProduceWithBrokerFailure"
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Transaction.Retry.Max = 200
	config.Consumer.IsolationLevel = ReadCommitted
	config.Net.MaxOpenRequests = 1

	consumer, err := NewConsumer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer consumer.Close()

	pc, err := consumer.ConsumePartition("test.1", 0, OffsetNewest)
	msgChannel := pc.Messages()
	require.NoError(t, err)
	defer pc.Close()

	nonTransactionalProducer, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, NewFunctionalTestConfig())
	require.NoError(t, err)
	defer nonTransactionalProducer.Close()

	// Ensure consumer is started
	nonTransactionalProducer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder("test")}
	<-msgChannel

	producer, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer producer.Close()

	txCoordinator, _ := producer.(*asyncProducer).client.TransactionCoordinator(config.Producer.Transaction.ID)

	err = producer.BeginTxn()
	require.NoError(t, err)

	if err := stopDockerTestBroker(context.Background(), txCoordinator.id); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := startDockerTestBroker(context.Background(), txCoordinator.id); err != nil {
			t.Fatal(err)
		}
		t.Logf("\n")
	}()

	for i := 0; i < 1; i++ {
		producer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder("test")}
	}

	err = producer.CommitTxn()
	require.NoError(t, err)

	for i := 0; i < 1; i++ {
		msg := <-msgChannel
		t.Logf("Received %s from %s-%d at offset %d", msg.Value, msg.Topic, msg.Partition, msg.Offset)
	}
}

func TestFuncTxnProduceEpochBump(t *testing.T) {
	checkKafkaVersion(t, "2.6.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.ChannelBufferSize = 20
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.Flush.Messages = 200
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "TestFuncTxnProduceEpochBump"
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Transaction.Retry.Max = 200
	config.Consumer.IsolationLevel = ReadCommitted
	config.Net.MaxOpenRequests = 1

	consumer, err := NewConsumer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer consumer.Close()

	pc, err := consumer.ConsumePartition("test.1", 0, OffsetNewest)
	msgChannel := pc.Messages()
	require.NoError(t, err)
	defer pc.Close()

	nonTransactionalProducer, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, NewFunctionalTestConfig())
	require.NoError(t, err)
	defer nonTransactionalProducer.Close()

	// Ensure consumer is started
	nonTransactionalProducer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder("test")}
	<-msgChannel

	producer, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer producer.Close()

	err = producer.BeginTxn()
	require.NoError(t, err)

	for i := 0; i < 1; i++ {
		producer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder("test")}
	}

	err = producer.CommitTxn()
	require.NoError(t, err)

	for i := 0; i < 1; i++ {
		msg := <-msgChannel
		t.Logf("Received %s from %s-%d at offset %d", msg.Value, msg.Topic, msg.Partition, msg.Offset)
	}

	err = producer.BeginTxn()
	require.NoError(t, err)

	for i := 0; i < 1; i++ {
		producer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder("test")}
	}

	err = producer.CommitTxn()
	require.NoError(t, err)

	for i := 0; i < 1; i++ {
		msg := <-msgChannel
		t.Logf("Received %s from %s-%d at offset %d", msg.Value, msg.Topic, msg.Partition, msg.Offset)
	}
}

func TestFuncInitProducerId3(t *testing.T) {
	checkKafkaVersion(t, "2.6.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.ChannelBufferSize = 20
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.Flush.Messages = 200
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "TestFuncInitProducerId3"
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Retry.Max = 50
	config.Consumer.IsolationLevel = ReadCommitted
	config.Net.MaxOpenRequests = 1

	producer, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer producer.Close()

	require.Equal(t, true, producer.(*asyncProducer).txnmgr.coordinatorSupportsBumpingEpoch)
}

type messageHandler struct {
	*testing.T
	h       func(*ConsumerMessage)
	started sync.WaitGroup
}

func (h *messageHandler) Setup(s ConsumerGroupSession) error   { return nil }
func (h *messageHandler) Cleanup(s ConsumerGroupSession) error { return nil }
func (h *messageHandler) ConsumeClaim(sess ConsumerGroupSession, claim ConsumerGroupClaim) error {
	h.started.Done()

	for msg := range claim.Messages() {
		h.Logf("consumed msg %v", msg)
		h.h(msg)
	}
	return nil
}

func TestFuncTxnProduceAndCommitOffset(t *testing.T) {
	checkKafkaVersion(t, "0.11.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.ChannelBufferSize = 20
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.Flush.Messages = 200
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "TestFuncTxnProduceAndCommitOffset"
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Transaction.Retry.Max = 200
	config.Consumer.IsolationLevel = ReadCommitted
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Net.MaxOpenRequests = 1

	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer client.Close()

	admin, err := NewClusterAdminFromClient(client)
	require.NoError(t, err)
	defer admin.Close()

	producer, err := NewAsyncProducerFromClient(client)
	require.NoError(t, err)
	defer producer.Close()

	cg, err := NewConsumerGroupFromClient("test-produce", client)
	require.NoError(t, err)
	defer cg.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &messageHandler{}
	handler.T = t
	handler.h = func(msg *ConsumerMessage) {
		err := producer.BeginTxn()
		require.NoError(t, err)
		producer.Input() <- &ProducerMessage{Topic: "test.1", Value: StringEncoder("test-prod")}
		err = producer.AddMessageToTxn(msg, "test-produce", nil)
		require.NoError(t, err)
		err = producer.CommitTxn()
		require.NoError(t, err)
	}

	handler.started.Add(4)
	go func() {
		err = cg.Consume(ctx, []string{"test.4"}, handler)
		require.NoError(t, err)
	}()

	handler.started.Wait()

	nonTransactionalProducer, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, NewFunctionalTestConfig())
	require.NoError(t, err)
	defer nonTransactionalProducer.Close()

	consumer, err := NewConsumerFromClient(client)
	require.NoError(t, err)
	defer consumer.Close()

	pc, err := consumer.ConsumePartition("test.1", 0, OffsetNewest)
	msgChannel := pc.Messages()
	require.NoError(t, err)
	defer pc.Close()

	// Ensure consumer is started
	nonTransactionalProducer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder("test")}
	<-msgChannel

	for i := 0; i < 1; i++ {
		nonTransactionalProducer.Input() <- &ProducerMessage{Topic: "test.4", Key: nil, Value: StringEncoder("test")}
	}

	for i := 0; i < 1; i++ {
		msg := <-msgChannel
		t.Logf("Received %s from %s-%d at offset %d", msg.Value, msg.Topic, msg.Partition, msg.Offset)
	}

	topicPartitions := make(map[string][]int32)
	topicPartitions["test.4"] = []int32{0, 1, 2, 3}
	topicsDescription, err := admin.ListConsumerGroupOffsets("test-produce", topicPartitions)
	require.NoError(t, err)

	for _, partition := range topicPartitions["test.4"] {
		block := topicsDescription.GetBlock("test.4", partition)
		_ = client.RefreshMetadata("test.4")
		lastOffset, err := client.GetOffset("test.4", partition, OffsetNewest)
		require.NoError(t, err)
		if block.Offset > -1 {
			require.Equal(t, lastOffset, block.Offset)
		}
	}
}

func TestFuncTxnProduceMultiTxn(t *testing.T) {
	checkKafkaVersion(t, "0.11.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.ChannelBufferSize = 20
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.Flush.Messages = 200
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "TestFuncTxnProduceMultiTxn"
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Transaction.Retry.Max = 200
	config.Consumer.IsolationLevel = ReadCommitted
	config.Net.MaxOpenRequests = 1

	configSecond := NewFunctionalTestConfig()
	configSecond.ChannelBufferSize = 20
	configSecond.Producer.Flush.Frequency = 50 * time.Millisecond
	configSecond.Producer.Flush.Messages = 200
	configSecond.Producer.Idempotent = true
	configSecond.Producer.Transaction.ID = "TestFuncTxnProduceMultiTxn-second"
	configSecond.Producer.RequiredAcks = WaitForAll
	configSecond.Producer.Retry.Max = 50
	configSecond.Consumer.IsolationLevel = ReadCommitted
	configSecond.Net.MaxOpenRequests = 1

	consumer, err := NewConsumer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer consumer.Close()

	pc, err := consumer.ConsumePartition("test.1", 0, OffsetNewest)
	msgChannel := pc.Messages()
	require.NoError(t, err)
	defer pc.Close()

	nonTransactionalConfig := NewFunctionalTestConfig()
	nonTransactionalConfig.Producer.Return.Successes = true
	nonTransactionalConfig.Producer.Return.Errors = true

	nonTransactionalProducer, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, nonTransactionalConfig)
	require.NoError(t, err)
	defer nonTransactionalProducer.Close()

	// Ensure consumer is started
	nonTransactionalProducer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder("test")}
	<-msgChannel

	producer, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer producer.Close()

	producerSecond, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, configSecond)
	require.NoError(t, err)
	defer producerSecond.Close()

	err = producer.BeginTxn()
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		producer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder("test-committed")}
	}

	err = producerSecond.BeginTxn()
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		producerSecond.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder("test-aborted")}
	}

	err = producer.CommitTxn()
	require.NoError(t, err)

	err = producerSecond.AbortTxn()
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		msg := <-msgChannel
		t.Logf("Received %s from %s-%d at offset %d", msg.Value, msg.Topic, msg.Partition, msg.Offset)
		require.Equal(t, "test-committed", string(msg.Value))
	}
}

func TestFuncTxnAbortedProduce(t *testing.T) {
	checkKafkaVersion(t, "0.11.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.ChannelBufferSize = 20
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.Flush.Messages = 200
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "TestFuncTxnAbortedProduce"
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Transaction.Retry.Max = 200
	config.Consumer.IsolationLevel = ReadCommitted
	config.Net.MaxOpenRequests = 1

	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer client.Close()

	consumer, err := NewConsumerFromClient(client)
	require.NoError(t, err)
	defer consumer.Close()

	pc, err := consumer.ConsumePartition("test.1", 0, OffsetNewest)
	msgChannel := pc.Messages()
	require.NoError(t, err)
	defer pc.Close()

	nonTransactionalConfig := NewFunctionalTestConfig()
	nonTransactionalConfig.Producer.Return.Successes = true
	nonTransactionalConfig.Producer.Return.Errors = true

	nonTransactionalProducer, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, nonTransactionalConfig)
	require.NoError(t, err)
	defer nonTransactionalProducer.Close()

	// Ensure consumer is started
	nonTransactionalProducer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder("test")}
	<-msgChannel

	producer, err := NewAsyncProducerFromClient(client)
	require.NoError(t, err)
	defer producer.Close()

	err = producer.BeginTxn()
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		producer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder("transactional")}
	}

	for i := 0; i < 2; i++ {
		<-producer.Successes()
	}

	err = producer.AbortTxn()
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		nonTransactionalProducer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder("non-transactional")}
		<-nonTransactionalProducer.Successes()
	}

	for i := 0; i < 2; i++ {
		msg := <-msgChannel
		t.Logf("Received %s from %s-%d at offset %d", msg.Value, msg.Topic, msg.Partition, msg.Offset)
		require.Equal(t, "non-transactional", string(msg.Value))
	}
}

func TestFuncProducingToInvalidTopic(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.Producer.Return.Successes = true
	producer, err := NewSyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := producer.SendMessage(&ProducerMessage{Topic: "in/valid"}); !errors.Is(err, ErrUnknownTopicOrPartition) && !errors.Is(err, ErrInvalidTopic) {
		t.Error("Expected ErrUnknownTopicOrPartition, found", err)
	}

	safeClose(t, producer)
}

func TestFuncProducingIdempotentWithBrokerFailure(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.Producer.Flush.Frequency = 250 * time.Millisecond
	config.Producer.Idempotent = true
	config.Producer.Timeout = 500 * time.Millisecond
	config.Producer.Retry.Max = 1
	config.Producer.Retry.Backoff = 500 * time.Millisecond
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = WaitForAll
	config.Net.MaxOpenRequests = 1

	producer, err := NewSyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, producer)

	// Successfully publish a few messages
	for i := 0; i < 10; i++ {
		_, _, err = producer.SendMessage(&ProducerMessage{
			Topic: "test.1",
			Value: StringEncoder(fmt.Sprintf("%d message", i)),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// break the brokers.
	for proxyName, proxy := range FunctionalTestEnv.Proxies {
		if !strings.Contains(proxyName, "kafka") {
			continue
		}
		if err := proxy.Disable(); err != nil {
			t.Fatal(err)
		}
	}

	// This should fail hard now
	for i := 10; i < 20; i++ {
		_, _, err = producer.SendMessage(&ProducerMessage{
			Topic: "test.1",
			Value: StringEncoder(fmt.Sprintf("%d message", i)),
		})
		if err == nil {
			t.Fatal(err)
		}
	}

	// Now bring the proxy back up
	for proxyName, proxy := range FunctionalTestEnv.Proxies {
		if !strings.Contains(proxyName, "kafka") {
			continue
		}
		if err := proxy.Enable(); err != nil {
			t.Fatal(err)
		}
	}

	// We should be able to publish again (once everything calms down)
	// (otherwise it times out)
	for {
		_, _, err = producer.SendMessage(&ProducerMessage{
			Topic: "test.1",
			Value: StringEncoder("comeback message"),
		})
		if err == nil {
			break
		}
	}
}

func TestInterceptors(t *testing.T) {
	config := NewFunctionalTestConfig()
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true
	config.Producer.Interceptors = []ProducerInterceptor{&appendInterceptor{i: 0}, &appendInterceptor{i: 100}}
	config.Consumer.Interceptors = []ConsumerInterceptor{&appendInterceptor{i: 20}}

	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, client)

	initialOffset, err := client.GetOffset("test.1", 0, OffsetNewest)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewAsyncProducerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder(TestMessage)}
	}

	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
		case msg := <-producer.Successes():
			v, _ := msg.Value.Encode()
			expected := TestMessage + strconv.Itoa(i) + strconv.Itoa(i+100)
			if string(v) != expected {
				t.Errorf("Interceptor should have incremented the value, got %s, expected %s", v, expected)
			}
		}
	}
	safeClose(t, producer)

	master, err := NewConsumerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := master.ConsumePartition("test.1", 0, initialOffset)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("Not received any more events in the last 10 seconds.")
		case err := <-consumer.Errors():
			t.Error(err)
		case msg := <-consumer.Messages():
			prodInteExpectation := strconv.Itoa(i) + strconv.Itoa(i+100)
			consInteExpectation := strconv.Itoa(i + 20)
			expected := TestMessage + prodInteExpectation + consInteExpectation
			v := string(msg.Value)
			if v != expected {
				t.Errorf("Interceptor should have incremented the value, got %s, expected %s", v, expected)
			}
		}
	}
	safeClose(t, consumer)
}

func testProducingMessages(t *testing.T, config *Config, minVersion KafkaVersion) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	// Configure some latency in order to properly validate the request latency metric
	for _, proxy := range FunctionalTestEnv.Proxies {
		if _, err := proxy.AddToxic("", "latency", "", 1, toxiproxy.Attributes{"latency": 10}); err != nil {
			t.Fatal("Unable to configure latency toxicity", err)
		}
	}

	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true

	kafkaVersions := map[KafkaVersion]bool{}
	upper, err := ParseKafkaVersion(os.Getenv("KAFKA_VERSION"))
	if err != nil {
		t.Logf("warning: failed to parse kafka version: %v", err)
	}
	if upper.IsAtLeast(minVersion) {
		kafkaVersions[upper] = true
		// KIP-896 dictates a minimum lower bound of 2.1 protocol for Kafka 4.0 onwards
		if upper.IsAtLeast(V4_0_0_0) {
			if !minVersion.IsAtLeast(V2_1_0_0) {
				minVersion = V2_1_0_0
			}
		}
	}

	for _, v := range []KafkaVersion{MinVersion, V0_10_0_0, V0_11_0_0, V1_0_0_0, V2_0_0_0, V2_1_0_0} {
		if v.IsAtLeast(minVersion) && upper.IsAtLeast(v) {
			kafkaVersions[v] = true
		}
	}

	for version := range kafkaVersions {
		name := t.Name() + "-v" + version.String()
		t.Run(name, func(t *testing.T) {
			config.ClientID = name
			config.MetricRegistry = metrics.NewRegistry()
			checkKafkaVersion(t, version.String())
			config.Version = version

			client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
			if err != nil {
				t.Fatal(err)
			}
			defer safeClose(t, client)

			// Keep in mind the current offset
			initialOffset, err := client.GetOffset("test.1", 0, OffsetNewest)
			if err != nil {
				t.Fatal(err)
			}

			producer, err := NewAsyncProducerFromClient(client)
			if err != nil {
				t.Fatal(err)
			}

			expectedResponses := TestBatchSize
			for i := 1; i <= TestBatchSize; {
				msg := &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder(fmt.Sprintf("testing %d", i))}
				select {
				case producer.Input() <- msg:
					i++
				case ret := <-producer.Errors():
					t.Fatal(ret.Err)
				case <-producer.Successes():
					expectedResponses--
				}
			}
			for expectedResponses > 0 {
				select {
				case ret := <-producer.Errors():
					t.Fatal(ret.Err)
				case <-producer.Successes():
					expectedResponses--
				}
			}
			safeClose(t, producer)

			// Validate producer metrics before using the consumer minus the offset request
			validateProducerMetrics(t, client)

			master, err := NewConsumerFromClient(client)
			if err != nil {
				t.Fatal(err)
			}
			consumer, err := master.ConsumePartition("test.1", 0, initialOffset)
			if err != nil {
				t.Fatal(err)
			}

			for i := 1; i <= TestBatchSize; i++ {
				select {
				case <-time.After(10 * time.Second):
					t.Fatal("Not received any more events in the last 10 seconds.")

				case err := <-consumer.Errors():
					t.Error(err)

				case message := <-consumer.Messages():
					if string(message.Value) != fmt.Sprintf("testing %d", i) {
						t.Fatalf("Unexpected message with index %d: %s", i, message.Value)
					}
				}
			}

			validateConsumerMetrics(t, client)

			safeClose(t, consumer)
		})
	}
}

// TestAsyncProducerRemoteBrokerClosed ensures that the async producer can
// cleanly recover if network connectivity to the remote brokers is lost and
// then subsequently resumed.
//
// https://github.com/IBM/sarama/issues/2129
func TestAsyncProducerRemoteBrokerClosed(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.ClientID = t.Name()
	config.Net.MaxOpenRequests = 1
	config.Producer.Flush.MaxMessages = 1
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 1024
	config.Producer.Retry.Backoff = time.Millisecond * 50

	producer, err := NewAsyncProducer(
		FunctionalTestEnv.KafkaBrokerAddrs,
		config,
	)
	if err != nil {
		t.Fatal(err)
	}

	// produce some more messages and ensure success
	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder(TestMessage)}
		<-producer.Successes()
	}

	// shutdown all the active tcp connections
	for _, proxy := range FunctionalTestEnv.Proxies {
		_ = proxy.Disable()
	}

	// produce some more messages
	for i := 10; i < 20; i++ {
		producer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder(TestMessage)}
	}

	// re-open the proxies
	for _, proxy := range FunctionalTestEnv.Proxies {
		_ = proxy.Enable()
	}

	// ensure the previously produced messages succeed
	for i := 10; i < 20; i++ {
		<-producer.Successes()
	}

	// produce some more messages and ensure success
	for i := 20; i < 30; i++ {
		producer.Input() <- &ProducerMessage{Topic: "test.1", Key: nil, Value: StringEncoder(TestMessage)}
		<-producer.Successes()
	}

	closeProducer(t, producer)
}

func validateProducerMetrics(t *testing.T, client Client) {
	// Get the broker used by test1 topic
	var broker *Broker
	if partitions, err := client.Partitions("test.1"); err != nil {
		t.Error(err)
	} else {
		for _, partition := range partitions {
			if b, err := client.Leader("test.1", partition); err != nil {
				t.Error(err)
			} else {
				if broker != nil && b != broker {
					t.Fatal("Expected only one broker, got at least 2")
				}
				broker = b
			}
		}
	}

	metricValidators := newMetricValidators()
	noResponse := client.Config().Producer.RequiredAcks == NoResponse
	compressionEnabled := client.Config().Producer.Compression != CompressionNone

	// We are adding 10ms of latency to all requests with toxiproxy
	minRequestLatencyInMs := 10
	if noResponse {
		// but when we do not wait for a response it can be less than 1ms
		minRequestLatencyInMs = 0
	}

	// We read at least 1 byte from the broker
	metricValidators.registerForAllBrokers(broker, minCountMeterValidator("incoming-byte-rate", 1))
	// in at least 3 global requests (1 for metadata request, 1 for offset request and N for produce request)
	metricValidators.register(minCountMeterValidator("request-rate", 3))
	metricValidators.register(minCountHistogramValidator("request-size", 3))
	metricValidators.register(minValHistogramValidator("request-size", 1))
	// and at least 2 requests to the registered broker (offset + produces)
	metricValidators.registerForBroker(broker, minCountMeterValidator("request-rate", 2))
	metricValidators.registerForBroker(broker, minCountHistogramValidator("request-size", 2))
	metricValidators.registerForBroker(broker, minValHistogramValidator("request-size", 1))
	metricValidators.registerForBroker(broker, minValHistogramValidator("request-latency-in-ms", minRequestLatencyInMs))

	// We send at least 1 batch
	metricValidators.registerForGlobalAndTopic("test_1", minCountHistogramValidator("batch-size", 1))
	metricValidators.registerForGlobalAndTopic("test_1", minValHistogramValidator("batch-size", 1))
	if compressionEnabled {
		// We record compression ratios between [0.50,-10.00] (50-1000 with a histogram) for at least one "fake" record
		metricValidators.registerForGlobalAndTopic("test_1", minCountHistogramValidator("compression-ratio", 1))
		if client.Config().Version.IsAtLeast(V0_11_0_0) {
			// slightly better compression with batching
			metricValidators.registerForGlobalAndTopic("test_1", minValHistogramValidator("compression-ratio", 30))
		} else {
			metricValidators.registerForGlobalAndTopic("test_1", minValHistogramValidator("compression-ratio", 50))
		}
		metricValidators.registerForGlobalAndTopic("test_1", maxValHistogramValidator("compression-ratio", 1000))
	} else {
		// We record compression ratios of 1.00 (100 with a histogram) for every TestBatchSize record
		if client.Config().Version.IsAtLeast(V0_11_0_0) {
			// records will be grouped in batchSet rather than msgSet
			metricValidators.registerForGlobalAndTopic("test_1", minCountHistogramValidator("compression-ratio", 3))
		} else {
			metricValidators.registerForGlobalAndTopic("test_1", countHistogramValidator("compression-ratio", TestBatchSize))
		}
		metricValidators.registerForGlobalAndTopic("test_1", minValHistogramValidator("compression-ratio", 100))
		metricValidators.registerForGlobalAndTopic("test_1", maxValHistogramValidator("compression-ratio", 100))
	}

	// We send exactly TestBatchSize messages
	metricValidators.registerForGlobalAndTopic("test_1", countMeterValidator("record-send-rate", TestBatchSize))
	// We send at least one record per request
	metricValidators.registerForGlobalAndTopic("test_1", minCountHistogramValidator("records-per-request", 1))
	metricValidators.registerForGlobalAndTopic("test_1", minValHistogramValidator("records-per-request", 1))

	// We receive at least 1 byte from the broker
	metricValidators.registerForAllBrokers(broker, minCountMeterValidator("outgoing-byte-rate", 1))
	if noResponse {
		// in exactly 2 global responses (metadata + offset)
		metricValidators.register(countMeterValidator("response-rate", 2))
		metricValidators.register(minCountHistogramValidator("response-size", 2))
		// and exactly 1 offset response for the registered broker
		metricValidators.registerForBroker(broker, countMeterValidator("response-rate", 1))
		metricValidators.registerForBroker(broker, minCountHistogramValidator("response-size", 1))
		metricValidators.registerForBroker(broker, minValHistogramValidator("response-size", 1))
	} else {
		// in at least 3 global responses (metadata + offset + produces)
		metricValidators.register(minCountMeterValidator("response-rate", 3))
		metricValidators.register(minCountHistogramValidator("response-size", 3))
		// and at least 2 for the registered broker
		metricValidators.registerForBroker(broker, minCountMeterValidator("response-rate", 2))
		metricValidators.registerForBroker(broker, minCountHistogramValidator("response-size", 2))
		metricValidators.registerForBroker(broker, minValHistogramValidator("response-size", 1))
	}

	// There should be no requests in flight anymore
	metricValidators.registerForAllBrokers(broker, counterValidator("requests-in-flight", 0))

	// Run the validators
	metricValidators.run(t, client.Config().MetricRegistry)
}

func validateConsumerMetrics(t *testing.T, client Client) {
	// Get the broker used by test1 topic
	var broker *Broker
	if partitions, err := client.Partitions("test.1"); err != nil {
		t.Error(err)
	} else {
		for _, partition := range partitions {
			if b, err := client.Leader("test.1", partition); err != nil {
				t.Error(err)
			} else {
				if broker != nil && b != broker {
					t.Fatal("Expected only one broker, got at least 2")
				}
				broker = b
			}
		}
	}

	metricValidators := newMetricValidators()

	// at least 1 global fetch request for the given topic
	metricValidators.registerForGlobalAndTopic("test_1", minCountMeterValidator("consumer-fetch-rate", 1))

	// and at least 1 fetch request to the lead broker
	metricValidators.registerForBroker(broker, minCountMeterValidator("consumer-fetch-rate", 1))

	// Run the validators
	metricValidators.run(t, client.Config().MetricRegistry)
}

// Benchmarks

func BenchmarkProducerSmall(b *testing.B) {
	benchmarkProducer(b, nil, "test.64", ByteEncoder(make([]byte, 128)))
}

func BenchmarkProducerMedium(b *testing.B) {
	benchmarkProducer(b, nil, "test.64", ByteEncoder(make([]byte, 1024)))
}

func BenchmarkProducerLarge(b *testing.B) {
	benchmarkProducer(b, nil, "test.64", ByteEncoder(make([]byte, 8192)))
}

func BenchmarkProducerSmallSinglePartition(b *testing.B) {
	benchmarkProducer(b, nil, "test.1", ByteEncoder(make([]byte, 128)))
}

func BenchmarkProducerMediumSnappy(b *testing.B) {
	conf := NewFunctionalTestConfig()
	conf.Producer.Compression = CompressionSnappy
	benchmarkProducer(b, conf, "test.1", ByteEncoder(make([]byte, 1024)))
}

func benchmarkProducer(b *testing.B, conf *Config, topic string, value Encoder) {
	setupFunctionalTest(b)
	defer teardownFunctionalTest(b)

	metricsDisable := os.Getenv("METRICS_DISABLE")
	if metricsDisable != "" {
		previousUseNilMetrics := metrics.UseNilMetrics
		Logger.Println("Disabling metrics using no-op implementation")
		metrics.UseNilMetrics = true
		// Restore previous setting
		defer func() {
			metrics.UseNilMetrics = previousUseNilMetrics
		}()
	}

	producer, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, conf)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 1; i <= b.N; {
		msg := &ProducerMessage{Topic: topic, Key: StringEncoder(fmt.Sprintf("%d", i)), Value: value}
		select {
		case producer.Input() <- msg:
			i++
		case ret := <-producer.Errors():
			b.Fatal(ret.Err)
		}
	}
	safeClose(b, producer)
}

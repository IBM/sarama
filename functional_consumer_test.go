//go:build functional

package sarama

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rcrowley/go-metrics"
	assert "github.com/stretchr/testify/require"
)

func TestFuncConsumerOffsetOutOfRange(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	consumer, err := NewConsumer(FunctionalTestEnv.KafkaBrokerAddrs, NewFunctionalTestConfig())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := consumer.ConsumePartition("test.1", 0, -10); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Error("Expected ErrOffsetOutOfRange, got:", err)
	}

	if _, err := consumer.ConsumePartition("test.1", 0, math.MaxInt64); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Error("Expected ErrOffsetOutOfRange, got:", err)
	}

	safeClose(t, consumer)
}

func TestConsumerHighWaterMarkOffset(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.Producer.Return.Successes = true

	p, err := NewSyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, p)

	_, offset, err := p.SendMessage(&ProducerMessage{Topic: "test.1", Value: StringEncoder("Test")})
	if err != nil {
		t.Fatal(err)
	}

	c, err := NewConsumer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, c)

	pc, err := c.ConsumePartition("test.1", 0, offset)
	if err != nil {
		t.Fatal(err)
	}

	<-pc.Messages()

	if hwmo := pc.HighWaterMarkOffset(); hwmo != offset+1 {
		t.Logf("Last produced offset %d; high water mark should be one higher but found %d.", offset, hwmo)
	}

	safeClose(t, pc)
}

// Makes sure that messages produced by all supported client versions/
// compression codecs (except LZ4) combinations can be consumed by all
// supported consumer versions. It relies on the KAFKA_VERSION environment
// variable to provide the version of the test Kafka cluster.
//
// Note that LZ4 codec was introduced in v0.10.0.0 and therefore is excluded
// from this test case. It has a similar version matrix test case below that
// only checks versions from v0.10.0.0 until KAFKA_VERSION.
func TestVersionMatrix(t *testing.T) {
	metrics.UseNilMetrics = true // disable Sarama's go-metrics library
	t.Cleanup(func() {
		metrics.UseNilMetrics = false
	})

	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	// Produce lot's of message with all possible combinations of supported
	// protocol versions and compressions for the except of LZ4.
	testVersions := versionRange(V0_8_2_0)
	allCodecsButLZ4 := []CompressionCodec{CompressionNone, CompressionGZIP, CompressionSnappy}
	producedMessages := produceMsgs(t, testVersions, allCodecsButLZ4, 17, 100, false)

	// When/Then
	consumeMsgs(t, testVersions, producedMessages)
}

// Support for LZ4 codec was introduced in v0.10.0.0 so a version matrix to
// test LZ4 should start with v0.10.0.0.
func TestVersionMatrixLZ4(t *testing.T) {
	metrics.UseNilMetrics = true // disable Sarama's go-metrics library
	t.Cleanup(func() {
		metrics.UseNilMetrics = false
	})
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	// Produce lot's of message with all possible combinations of supported
	// protocol versions starting with v0.10 (first where LZ4 was supported)
	// and all possible compressions.
	testVersions := versionRange(V0_10_0_0)
	allCodecs := []CompressionCodec{CompressionNone, CompressionGZIP, CompressionSnappy, CompressionLZ4}
	producedMessages := produceMsgs(t, testVersions, allCodecs, 17, 100, false)

	// When/Then
	consumeMsgs(t, testVersions, producedMessages)
}

// Support for zstd codec was introduced in v2.1.0.0
func TestVersionMatrixZstd(t *testing.T) {
	checkKafkaVersion(t, "2.1.0")
	metrics.UseNilMetrics = true // disable Sarama's go-metrics library
	t.Cleanup(func() {
		metrics.UseNilMetrics = false
	})
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	// Produce lot's of message with all possible combinations of supported
	// protocol versions starting with v2.1.0.0 (first where zstd was supported)
	testVersions := versionRange(V2_1_0_0)
	allCodecs := []CompressionCodec{CompressionZSTD}
	producedMessages := produceMsgs(t, testVersions, allCodecs, 17, 100, false)

	// When/Then
	consumeMsgs(t, testVersions, producedMessages)
}

func TestVersionMatrixIdempotent(t *testing.T) {
	metrics.UseNilMetrics = true // disable Sarama's go-metrics library
	t.Cleanup(func() {
		metrics.UseNilMetrics = false
	})
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	// Produce lot's of message with all possible combinations of supported
	// protocol versions starting with v0.11 (first where idempotent was supported)
	testVersions := versionRange(V0_11_0_0)
	producedMessages := produceMsgs(t, testVersions, []CompressionCodec{CompressionNone}, 17, 100, true)

	// When/Then
	consumeMsgs(t, testVersions, producedMessages)
}

func TestReadOnlyAndAllCommittedMessages(t *testing.T) {
	t.Skip("TODO: TestReadOnlyAndAllCommittedMessages is periodically failing inexplicably.")
	checkKafkaVersion(t, "0.11.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.ClientID = t.Name()
	config.Net.MaxOpenRequests = 1
	config.Consumer.IsolationLevel = ReadCommitted
	config.Producer.Idempotent = true
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = WaitForAll

	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	controller, err := client.Controller()
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()

	transactionalID := strconv.FormatInt(time.Now().UnixNano()/(1<<22), 10)

	var coordinator *Broker

	// find the transaction coordinator
	for {
		coordRes, err := controller.FindCoordinator(&FindCoordinatorRequest{
			Version:         2,
			CoordinatorKey:  transactionalID,
			CoordinatorType: CoordinatorTransaction,
		})
		if err != nil {
			t.Fatal(err)
		}
		if coordRes.Err != ErrNoError {
			continue
		}
		if err := coordRes.Coordinator.Open(client.Config()); err != nil {
			t.Fatal(err)
		}
		coordinator = coordRes.Coordinator
		break
	}

	// produce some uncommitted messages to the topic
	pidRes, err := coordinator.InitProducerID(&InitProducerIDRequest{
		TransactionalID:    &transactionalID,
		TransactionTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = coordinator.AddPartitionsToTxn(&AddPartitionsToTxnRequest{
		TransactionalID: transactionalID,
		ProducerID:      pidRes.ProducerID,
		ProducerEpoch:   pidRes.ProducerEpoch,
		TopicPartitions: map[string][]int32{
			uncommittedTopic: {0},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ps := &produceSet{
		msgs: make(map[string]map[int32]*partitionSet),
		parent: &asyncProducer{
			conf:   config,
			txnmgr: &transactionManager{},
		},
		producerID:    pidRes.ProducerID,
		producerEpoch: pidRes.ProducerEpoch,
	}
	_ = ps.add(&ProducerMessage{
		Topic:     uncommittedTopic,
		Partition: 0,
		Value:     StringEncoder("uncommitted message 1"),
	})
	_ = ps.add(&ProducerMessage{
		Topic:     uncommittedTopic,
		Partition: 0,
		Value:     StringEncoder("uncommitted message 2"),
	})
	produceReq := ps.buildRequest()
	produceReq.TransactionalID = &transactionalID
	if resp, err := coordinator.Produce(produceReq); err != nil {
		t.Fatal(err)
	} else {
		b := resp.GetBlock(uncommittedTopic, 0)
		if b != nil {
			t.Logf("uncommitted message 1 to %s-%d at offset %d", uncommittedTopic, 0, b.Offset)
			t.Logf("uncommitted message 2 to %s-%d at offset %d", uncommittedTopic, 0, b.Offset+1)
		}
	}

	// now produce some committed messages to the topic
	producer, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	for i := 1; i <= 6; i++ {
		producer.Input() <- &ProducerMessage{
			Topic:     uncommittedTopic,
			Partition: 0,
			Value:     StringEncoder(fmt.Sprintf("Committed %v", i)),
		}
		msg := <-producer.Successes()
		t.Logf("Committed %v to %s-%d at offset %d", i, msg.Topic, msg.Partition, msg.Offset)
	}

	// now abort the uncommitted transaction
	if _, err := coordinator.EndTxn(&EndTxnRequest{
		TransactionalID:   transactionalID,
		ProducerID:        pidRes.ProducerID,
		ProducerEpoch:     pidRes.ProducerEpoch,
		TransactionResult: false, // aborted
	}); err != nil {
		t.Fatal(err)
	}

	consumer, err := NewConsumer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(uncommittedTopic, 0, OffsetOldest)
	assert.NoError(t, err)

	msgChannel := pc.Messages()
	for i := 1; i <= 6; i++ {
		msg := <-msgChannel
		t.Logf("Received %s from %s-%d at offset %d", msg.Value, msg.Topic, msg.Partition, msg.Offset)
		assert.Equal(t, fmt.Sprintf("Committed %v", i), string(msg.Value))
	}
}

func TestConsumerGroupDeadlock(t *testing.T) {
	checkKafkaVersion(t, "1.1.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	const topic = "test_consumer_group_rebalance_test_topic"
	const msgQty = 50
	partitionsQty := len(FunctionalTestEnv.KafkaBrokerAddrs) * 3

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	config := NewFunctionalTestConfig()
	config.Version = V1_1_0_0
	config.ClientID = t.Name()
	config.Producer.Return.Successes = true
	config.ChannelBufferSize = 2 * msgQty

	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	assert.NoError(t, err)

	admin, err := NewClusterAdminFromClient(client)
	assert.NoError(t, err)

	cgName := "test_consumer_group_rebalance_consumer_group"

	err = admin.DeleteConsumerGroup(cgName)
	if err != nil {
		t.Logf("failed to delete topic: %s", err)
	}

	err = admin.DeleteTopic(topic)
	if err != nil {
		t.Logf("failed to delete topic: %s", err)
	}

	// it takes time to delete topic, the API is not sync
	for i := 0; i < 5; i++ {
		err = admin.CreateTopic(topic, &TopicDetail{NumPartitions: int32(partitionsQty), ReplicationFactor: 1}, false)
		if err == nil {
			break
		}
		if errors.Is(err, ErrTopicAlreadyExists) || strings.Contains(err.Error(), "is marked for deletion") {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		break
	}
	assert.NoError(t, err)
	defer func() {
		_ = admin.DeleteTopic(topic)
	}()

	var wg sync.WaitGroup

	consumer, err := NewConsumerFromClient(client)
	assert.NoError(t, err)

	ch := make(chan string, msgQty)
	for i := 0; i < partitionsQty; i++ {
		time.Sleep(250 * time.Millisecond) // ensure delays between the "claims"
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			pConsumer, err := consumer.ConsumePartition(topic, int32(i), OffsetOldest)
			assert.NoError(t, err)
			defer pConsumer.Close()

			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-pConsumer.Messages():
					if !ok {
						return
					}
					// t.Logf("consumer-group %d consumed: %v from %s/%d/%d", i, msg.Value, msg.Topic, msg.Partition, msg.Offset)
					ch <- string(msg.Value)
				}
			}
		}(i)
	}

	producer, err := NewSyncProducerFromClient(client)
	assert.NoError(t, err)

	for i := 0; i < msgQty; i++ {
		msg := &ProducerMessage{
			Topic: topic,
			Value: StringEncoder(strconv.FormatInt(int64(i), 10)),
		}
		_, _, err := producer.SendMessage(msg)
		assert.NoError(t, err)
	}

	var received []string
	func() {
		for len(received) < msgQty {
			select {
			case <-ctx.Done():
				return
			case msg := <-ch:
				received = append(received, msg)
				// t.Logf("received: %s, count: %d", msg, len(received))
			}
		}
	}()

	cancel()

	assert.Equal(t, msgQty, len(received))

	err = producer.Close()
	assert.NoError(t, err)

	err = consumer.Close()
	assert.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)

	wg.Wait()
}

func prodMsg2Str(prodMsg *ProducerMessage) string {
	return fmt.Sprintf("{offset: %d, value: %s}", prodMsg.Offset, string(prodMsg.Value.(StringEncoder)))
}

func consMsg2Str(consMsg *ConsumerMessage) string {
	return fmt.Sprintf("{offset: %d, value: %s}", consMsg.Offset, string(consMsg.Value))
}

func versionRange(lower KafkaVersion) []KafkaVersion {
	// Get the test cluster version from the environment. If there is nothing
	// there then assume the highest.
	upper, err := ParseKafkaVersion(os.Getenv("KAFKA_VERSION"))
	if err != nil {
		upper = MaxVersion
	}

	// KIP-896 dictates a minimum lower bound of 2.1 protocol for Kafka 4.0 onwards
	if upper.IsAtLeast(V4_0_0_0) {
		if !lower.IsAtLeast(V2_1_0_0) {
			lower = V2_1_0_0
		}
	}

	versions := make([]KafkaVersion, 0, len(fvtRangeVersions))
	for _, v := range fvtRangeVersions {
		if !v.IsAtLeast(lower) {
			continue
		}
		if !upper.IsAtLeast(v) {
			return versions
		}
		versions = append(versions, v)
	}
	return versions
}

func produceMsgs(t *testing.T, clientVersions []KafkaVersion, codecs []CompressionCodec, flush int, countPerVerCodec int, idempotent bool) []*ProducerMessage {
	var (
		producers          []SyncProducer
		producedMessagesMu sync.Mutex
		producedMessages   []*ProducerMessage
	)
	g := errgroup.Group{}
	for _, prodVer := range clientVersions {
		for _, codec := range codecs {
			prodCfg := NewFunctionalTestConfig()
			prodCfg.ClientID = t.Name() + "-Producer-" + prodVer.String()
			if idempotent {
				prodCfg.ClientID += "-idempotent"
			}
			if codec > 0 {
				prodCfg.ClientID += "-" + codec.String()
			}
			prodCfg.Metadata.Full = false
			prodCfg.Version = prodVer
			prodCfg.Producer.Return.Successes = true
			prodCfg.Producer.Return.Errors = true
			prodCfg.Producer.Flush.MaxMessages = flush
			prodCfg.Producer.Compression = codec
			prodCfg.Producer.Idempotent = idempotent
			if idempotent {
				prodCfg.Producer.RequiredAcks = WaitForAll
				prodCfg.Net.MaxOpenRequests = 1
			}

			p, err := NewSyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, prodCfg)
			if err != nil {
				t.Fatalf("Failed to create producer: version=%s, compression=%s, err=%v", prodVer, codec, err)
			}
			producers = append(producers, p)

			prodVer := prodVer
			codec := codec
			g.Go(func() error {
				t.Logf("*** Producing with client version %s codec %s\n", prodVer, codec)
				var wg sync.WaitGroup
				for i := 0; i < countPerVerCodec; i++ {
					msg := &ProducerMessage{
						Topic: "test.1",
						Value: StringEncoder(fmt.Sprintf("msg:%s:%s:%d", prodVer, codec, i)),
					}
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, _, err := p.SendMessage(msg)
						if err != nil {
							t.Errorf("Failed to produce message: %s, err=%v", msg.Value, err)
						}
						producedMessagesMu.Lock()
						producedMessages = append(producedMessages, msg)
						producedMessagesMu.Unlock()
					}()
				}
				wg.Wait()
				return nil
			})
		}
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}

	for _, p := range producers {
		safeClose(t, p)
	}

	// Sort produced message in ascending offset order.
	sort.Slice(producedMessages, func(i, j int) bool {
		return producedMessages[i].Offset < producedMessages[j].Offset
	})
	assert.NotEmpty(t, producedMessages, "should have produced >0 messages")
	t.Logf("*** Total produced %d, firstOffset=%d, lastOffset=%d\n",
		len(producedMessages), producedMessages[0].Offset, producedMessages[len(producedMessages)-1].Offset)
	return producedMessages
}

func consumeMsgs(t *testing.T, clientVersions []KafkaVersion, producedMessages []*ProducerMessage) {
	// Consume all produced messages with all client versions supported by the
	// cluster.
	g := errgroup.Group{}
	for _, consVer := range clientVersions {
		// Create a partition consumer that should start from the first produced
		// message.
		consCfg := NewFunctionalTestConfig()
		consCfg.ClientID = t.Name() + "-Consumer-" + consVer.String()
		consCfg.Consumer.MaxProcessingTime = time.Second
		consCfg.Metadata.Full = false
		consCfg.Version = consVer
		c, err := NewConsumer(FunctionalTestEnv.KafkaBrokerAddrs, consCfg)
		if err != nil {
			t.Fatal(err)
		}
		defer safeClose(t, c) //nolint: gocritic // the close intentionally happens outside the loop
		pc, err := c.ConsumePartition("test.1", 0, producedMessages[0].Offset)
		if err != nil {
			t.Fatal(err)
		}
		defer safeClose(t, pc) //nolint: gocritic // the close intentionally happens outside the loop

		var wg sync.WaitGroup
		wg.Add(1)
		consVer := consVer
		g.Go(func() error {
			// Consume as many messages as there have been produced and make sure that
			// order is preserved.
			t.Logf("*** Consuming with client version %s\n", consVer)
			for i, prodMsg := range producedMessages {
				select {
				case consMsg := <-pc.Messages():
					if consMsg.Offset != prodMsg.Offset {
						t.Fatalf("Consumed unexpected offset: version=%s, index=%d, want=%s, got=%s",
							consVer, i, prodMsg2Str(prodMsg), consMsg2Str(consMsg))
					}
					if string(consMsg.Value) != string(prodMsg.Value.(StringEncoder)) {
						t.Fatalf("Consumed unexpected msg: version=%s, index=%d, want=%s, got=%s",
							consVer, i, prodMsg2Str(prodMsg), consMsg2Str(consMsg))
					}
					if i == 0 {
						t.Logf("Consumed first msg: version=%s, index=%d, got=%s",
							consVer, i, consMsg2Str(consMsg))
						wg.Done()
					}
					if i%1000 == 0 {
						t.Logf("Consumed messages: version=%s, index=%d, got=%s",
							consVer, i, consMsg2Str(consMsg))
					}
				case <-time.After(15 * time.Second):
					t.Fatalf("Timeout %s waiting for: index=%d, offset=%d, msg=%s",
						consCfg.ClientID, i, prodMsg.Offset, prodMsg.Value)
				}
			}
			return nil
		})
		wg.Wait() // wait for first message to be consumed before starting next consumer
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
}

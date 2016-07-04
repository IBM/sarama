package sarama

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"
)

const TestBatchSize = 1000

func TestFuncProducing(t *testing.T) {
	config := NewConfig()
	testProducingMessages(t, config)
}

func TestFuncProducingGzip(t *testing.T) {
	config := NewConfig()
	config.Producer.Compression = CompressionGZIP
	testProducingMessages(t, config)
}

func TestFuncProducingSnappy(t *testing.T) {
	config := NewConfig()
	config.Producer.Compression = CompressionSnappy
	testProducingMessages(t, config)
}

func TestFuncProducingNoResponse(t *testing.T) {
	config := NewConfig()
	config.Producer.RequiredAcks = NoResponse
	testProducingMessages(t, config)
}

func TestFuncProducingFlushing(t *testing.T) {
	config := NewConfig()
	config.Producer.Flush.Messages = TestBatchSize / 8
	config.Producer.Flush.Frequency = 250 * time.Millisecond
	testProducingMessages(t, config)
}

func TestFuncMultiPartitionProduce(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewConfig()
	config.ChannelBufferSize = 20
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.Flush.Messages = 200
	config.Producer.Return.Successes = true
	producer, err := NewSyncProducer(kafkaBrokers, config)
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

func TestFuncProducingToInvalidTopic(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	producer, err := NewSyncProducer(kafkaBrokers, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := producer.SendMessage(&ProducerMessage{Topic: "in/valid"}); err != ErrUnknownTopicOrPartition {
		t.Error("Expected ErrUnknownTopicOrPartition, found", err)
	}

	if _, _, err := producer.SendMessage(&ProducerMessage{Topic: "in/valid"}); err != ErrUnknownTopicOrPartition {
		t.Error("Expected ErrUnknownTopicOrPartition, found", err)
	}

	safeClose(t, producer)
}

func testProducingMessages(t *testing.T, config *Config) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	// Use a dedicated registry to prevent side effect caused by the global one
	config.MetricRegistry = metrics.NewRegistry()

	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true

	client, err := NewClient(kafkaBrokers, config)
	if err != nil {
		t.Fatal(err)
	}

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

	// Validate producer metrics before using the consumer
	validateMetrics(t, client)

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
	safeClose(t, consumer)
	safeClose(t, client)
}

func validateMetrics(t *testing.T, client Client) {
	// Get the broker ids used by test1 topic
	brokerIds := make(map[int32]bool)
	if partitions, err := client.Partitions("test.1"); err != nil {
		t.Error(err)
	} else {
		for _, partition := range partitions {
			if broker, err := client.Leader("test.1", partition); err != nil {
				t.Error(err)
			} else {
				brokerIds[broker.ID()] = true
			}
		}
	}

	minCountMeterValidatorBuilder := func(minCount int64) func(string, interface{}) {
		return func(name string, metric interface{}) {
			if meter, ok := metric.(metrics.Meter); !ok {
				t.Errorf("Expected meter metric for '%s', got %T", name, metric)
			} else {
				count := meter.Count()
				if count < minCount {
					t.Errorf("Expected meter metric '%s' count >= %d, got %d", name, minCount, count)
				}
			}
		}
	}

	histogramValidatorBuilder := func(minCount int64, minMin int64, maxMax int64) func(string, interface{}) {
		return func(name string, metric interface{}) {
			if histogram, ok := metric.(metrics.Histogram); !ok {
				t.Errorf("Expected histogram metric for '%s', got %T", name, metric)
			} else {
				count := histogram.Count()
				if count < minCount {
					t.Errorf("Expected histogram metric '%s' count >= %d, got %d", name, minCount, count)
				}
				min := int64(histogram.Min())
				if min < minMin {
					t.Errorf("Expected histogram metric '%s' min >= %d, got %d", name, minMin, min)
				}
				max := int64(histogram.Max())
				if max > maxMax {
					t.Errorf("Expected histogram metric '%s' max <= %d, got %d", name, maxMax, max)
				}
			}
		}
	}

	type expectedMetric struct {
		name      string
		validator func(string, interface{})
	}
	expectedMetrics := make([]expectedMetric, 0, 20)
	addExpectedBrokerMetric := func(name string, validator func(string, interface{})) {
		expectedMetrics = append(expectedMetrics, expectedMetric{name, validator})
		for brokerId, _ := range brokerIds {
			expectedMetrics = append(expectedMetrics, expectedMetric{fmt.Sprintf("%s-for-broker-%d", name, brokerId), validator})
		}
	}
	addExpectedTopicMetric := func(name string, validator func(string, interface{})) {
		expectedMetrics = append(expectedMetrics, expectedMetric{name, validator})
		expectedMetrics = append(expectedMetrics, expectedMetric{fmt.Sprintf("%s-for-topic-test.1", name), validator})
	}

	compressionEnabled := client.Config().Producer.Compression != CompressionNone
	noResponse := client.Config().Producer.RequiredAcks == NoResponse

	// We read at least 1 byte from the brokers
	addExpectedBrokerMetric("incoming-byte-rate", minCountMeterValidatorBuilder(1))
	// in at least 2 requests to the brokers (1 for metadata request and N for produce request)
	addExpectedBrokerMetric("request-rate", minCountMeterValidatorBuilder(2)) // Count 15 / 8 for broker
	addExpectedBrokerMetric("request-size", histogramValidatorBuilder(2, 1, math.MaxInt64))
	// We receive at least 1 byte from the broker
	addExpectedBrokerMetric("outgoing-byte-rate", minCountMeterValidatorBuilder(1))
	if noResponse {
		// in a single response (metadata)
		addExpectedBrokerMetric("response-rate", minCountMeterValidatorBuilder(1))
		addExpectedBrokerMetric("response-size", histogramValidatorBuilder(1, 1, math.MaxInt64))
	} else {
		// in at least 2 responses (metadata + produce)
		addExpectedBrokerMetric("response-rate", minCountMeterValidatorBuilder(2))
		addExpectedBrokerMetric("response-size", histogramValidatorBuilder(2, 1, math.MaxInt64))
	}

	// We send at least 1 batch
	addExpectedTopicMetric("batch-size", histogramValidatorBuilder(1, 1, math.MaxInt64))
	if compressionEnabled {
		// We record compression ratio 0.01-2.00 (1-200 with a histogram) for at least one fake message
		addExpectedTopicMetric("compression-rate", histogramValidatorBuilder(1, 1, 200))
	} else {
		// We record compression ratio 1.00 (100 with a histogram) for every record
		addExpectedTopicMetric("compression-rate", histogramValidatorBuilder(TestBatchSize, 100, 100))
	}
	// We send exactly TestBatchSize messages
	addExpectedTopicMetric("record-send-rate", minCountMeterValidatorBuilder(TestBatchSize))
	// We send at least one record per request
	addExpectedTopicMetric("records-per-request", histogramValidatorBuilder(1, 1, math.MaxInt64))

	for _, expectedMetric := range expectedMetrics {
		foundMetric := client.Config().MetricRegistry.Get(expectedMetric.name)
		if foundMetric == nil {
			t.Error("No metric named", expectedMetric.name)
		} else {
			expectedMetric.validator(expectedMetric.name, foundMetric)
		}
	}
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
	conf := NewConfig()
	conf.Producer.Compression = CompressionSnappy
	benchmarkProducer(b, conf, "test.1", ByteEncoder(make([]byte, 1024)))
}

func benchmarkProducer(b *testing.B, conf *Config, topic string, value Encoder) {
	setupFunctionalTest(b)
	defer teardownFunctionalTest(b)

	producer, err := NewAsyncProducer(kafkaBrokers, conf)
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

package sarama

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	TestBatchSize = 1000

	VagrantKafkaPeers     = "192.168.100.67:6667,192.168.100.67:6668,192.168.100.67:6669,192.168.100.67:6670,192.168.100.67:6671"
	VagrantZookeeperPeers = "192.168.100.67:2181,192.168.100.67:2182,192.168.100.67:2183,192.168.100.67:2184,192.168.100.67:2185"
)

var (
	kafkaIsAvailable, kafkaShouldBeAvailable bool
	kafkaBrokers                             []string
)

func init() {
	kafkaPeers := os.Getenv("KAFKA_PEERS")
	if kafkaPeers == "" {
		kafkaPeers = VagrantKafkaPeers
	}
	kafkaBrokers = strings.Split(kafkaPeers, ",")

	if c, err := net.DialTimeout("tcp", kafkaBrokers[0], 5*time.Second); err == nil {
		if err = c.Close(); err == nil {
			kafkaIsAvailable = true
		}
	}

	kafkaShouldBeAvailable = os.Getenv("CI") != ""

	if os.Getenv("DEBUG") == "true" {
		Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}
}

func checkKafkaAvailability(t *testing.T) {
	if !kafkaIsAvailable {
		if kafkaShouldBeAvailable {
			t.Fatalf("Kafka broker is not available on %s. Set KAFKA_PEERS to connect to Kafka on a different location.", kafkaBrokers[0])
		} else {
			t.Skipf("Kafka broker is not available on %s. Set KAFKA_PEERS to connect to Kafka on a different location.", kafkaBrokers[0])
		}
	}
}

func TestFuncConnectionFailure(t *testing.T) {
	config := NewConfig()
	config.Metadata.Retry.Max = 1

	_, err := NewClient([]string{"localhost:9000"}, config)
	if err != ErrOutOfBrokers {
		t.Fatal("Expected returned error to be ErrOutOfBrokers, but was: ", err)
	}
}

func TestFuncClientMetadata(t *testing.T) {
	checkKafkaAvailability(t)

	config := NewConfig()
	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 10 * time.Millisecond
	client, err := NewClient(kafkaBrokers, config)
	if err != nil {
		t.Fatal(err)
	}

	if err := client.RefreshMetadata("unknown_topic"); err != ErrUnknownTopicOrPartition {
		t.Error("Expected ErrUnknownTopicOrPartition, got", err)
	}

	if _, err := client.Leader("unknown_topic", 0); err != ErrUnknownTopicOrPartition {
		t.Error("Expected ErrUnknownTopicOrPartition, got", err)
	}

	if _, err := client.Replicas("invalid/topic", 0); err != ErrUnknownTopicOrPartition {
		t.Error("Expected ErrUnknownTopicOrPartition, got", err)
	}

	partitions, err := client.Partitions("multi_partition")
	if err != nil {
		t.Error(err)
	}
	if len(partitions) != 2 {
		t.Errorf("Expected multi_partition topic to have 2 partitions, found %v", partitions)
	}

	partitions, err = client.Partitions("single_partition")
	if err != nil {
		t.Error(err)
	}
	if len(partitions) != 1 {
		t.Errorf("Expected single_partition topic to have 1 partitions, found %v", partitions)
	}

	safeClose(t, client)
}

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
	checkKafkaAvailability(t)

	config := NewConfig()
	config.ChannelBufferSize = 20
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.Flush.Messages = 200
	config.Producer.Return.Successes = true
	producer, err := NewAsyncProducer(kafkaBrokers, config)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(TestBatchSize)

	for i := 1; i <= TestBatchSize; i++ {

		go func(i int, w *sync.WaitGroup) {
			defer w.Done()
			msg := &ProducerMessage{Topic: "multi_partition", Key: nil, Value: StringEncoder(fmt.Sprintf("hur %d", i))}
			producer.Input() <- msg
			select {
			case ret := <-producer.Errors():
				t.Fatal(ret.Err)
			case <-producer.Successes():
			}
		}(i, &wg)
	}

	wg.Wait()
	if err := producer.Close(); err != nil {
		t.Error(err)
	}
}

func TestProducingToInvalidTopic(t *testing.T) {
	checkKafkaAvailability(t)

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
	checkKafkaAvailability(t)

	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true

	client, err := NewClient(kafkaBrokers, config)
	if err != nil {
		t.Fatal(err)
	}

	master, err := NewConsumerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := master.ConsumePartition("single_partition", 0, OffsetNewest)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewAsyncProducerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}

	expectedResponses := TestBatchSize
	for i := 1; i <= TestBatchSize; {
		msg := &ProducerMessage{Topic: "single_partition", Key: nil, Value: StringEncoder(fmt.Sprintf("testing %d", i))}
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

package sarama

import (
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	TestBatchSize = 1000
)

var (
	kafkaIsAvailable, kafkaShouldBeAvailable bool
	kafkaAddr                                string
)

func init() {
	kafkaAddr = os.Getenv("KAFKA_ADDR")
	if kafkaAddr == "" {
		kafkaAddr = "localhost:6667"
	}

	c, err := net.Dial("tcp", kafkaAddr)
	if err == nil {
		kafkaIsAvailable = true
		c.Close()
	}

	kafkaShouldBeAvailable = os.Getenv("CI") != ""
}

func checkKafkaAvailability(t *testing.T) {
	if !kafkaIsAvailable {
		if kafkaShouldBeAvailable {
			t.Fatalf("Kafka broker is not available on %s. Set KAFKA_ADDR to connect to Kafka on a different location.", kafkaAddr)
		} else {
			t.Skipf("Kafka broker is not available on %s. Set KAFKA_ADDR to connect to Kafka on a different location.", kafkaAddr)
		}
	}
}

func TestFuncConnectionFailure(t *testing.T) {
	config := NewClientConfig()
	config.MetadataRetries = 1

	_, err := NewClient("test", []string{"localhost:9000"}, config)
	if err != OutOfBrokers {
		t.Fatal("Expected returned error to be OutOfBrokers, but was: ", err)
	}
}

func TestFuncProducing(t *testing.T) {
	config := NewProducerConfig()
	testProducingMessages(t, config)
}

func TestFuncProducingGzip(t *testing.T) {
	config := NewProducerConfig()
	config.Compression = CompressionGZIP
	testProducingMessages(t, config)
}

func TestFuncProducingSnappy(t *testing.T) {
	config := NewProducerConfig()
	config.Compression = CompressionSnappy
	testProducingMessages(t, config)
}

func TestFuncProducingNoResponse(t *testing.T) {
	config := NewProducerConfig()
	config.RequiredAcks = NoResponse
	testProducingMessages(t, config)
}

func TestFuncProducingFlushing(t *testing.T) {
	config := NewProducerConfig()
	config.FlushMsgCount = TestBatchSize / 8
	config.FlushFrequency = 250 * time.Millisecond
	testProducingMessages(t, config)
}

func TestFuncMultiPartitionProduce(t *testing.T) {
	checkKafkaAvailability(t)
	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, client)

	config := NewProducerConfig()
	config.FlushFrequency = 50 * time.Millisecond
	config.FlushMsgCount = 200
	config.ChannelBufferSize = 20
	config.AckSuccesses = true
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(TestBatchSize)

	for i := 1; i <= TestBatchSize; i++ {

		go func(i int, w *sync.WaitGroup) {
			defer w.Done()
			msg := &MessageToSend{Topic: "multi_partition", Key: nil, Value: StringEncoder(fmt.Sprintf("hur %d", i))}
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

func testProducingMessages(t *testing.T, config *ProducerConfig) {
	checkKafkaAvailability(t)

	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}

	master, err := NewConsumer(client, nil)
	if err != nil {
		t.Fatal(err)
	}
	consumerConfig := NewPartitionConsumerConfig()
	consumerConfig.OffsetMethod = OffsetMethodNewest
	consumer, err := master.ConsumePartition("single_partition", 0, consumerConfig)
	if err != nil {
		t.Fatal(err)
	}

	config.AckSuccesses = true
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}

	expectedResponses := TestBatchSize
	for i := 1; i <= TestBatchSize; {
		msg := &MessageToSend{Topic: "single_partition", Key: nil, Value: StringEncoder(fmt.Sprintf("testing %d", i))}
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

	events := consumer.Events()
	for i := 1; i <= TestBatchSize; i++ {
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("Not received any more events in the last 10 seconds.")

		case event := <-events:
			if string(event.Value) != fmt.Sprintf("testing %d", i) {
				t.Fatalf("Unexpected message with index %d: %s", i, event.Value)
			}
		}

	}
	safeClose(t, consumer)
	safeClose(t, client)
}

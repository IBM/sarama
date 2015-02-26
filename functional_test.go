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

	if c, err := net.Dial("tcp", kafkaAddr); err == nil {
		if err = c.Close(); err == nil {
			kafkaIsAvailable = true
		}
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
	config := NewConfig()
	config.Metadata.Retries = 1

	_, err := NewClient([]string{"localhost:9000"}, config)
	if err != ErrOutOfBrokers {
		t.Fatal("Expected returned error to be ErrOutOfBrokers, but was: ", err)
	}
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
	client, err := NewClient([]string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, client)

	config := NewConfig()
	config.ChannelBufferSize = 20
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.Flush.Messages = 200
	config.Producer.AckSuccesses = true
	producer, err := NewProducer(client, config)
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

func testProducingMessages(t *testing.T, config *Config) {
	checkKafkaAvailability(t)

	client, err := NewClient([]string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}

	master, err := NewConsumer(client, nil)
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := master.ConsumePartition("single_partition", 0, OffsetMethodNewest, 0)
	if err != nil {
		t.Fatal(err)
	}

	config.Producer.AckSuccesses = true
	producer, err := NewProducer(client, config)
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

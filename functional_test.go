package sarama

import (
	"fmt"
	"net"
	"os"
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
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}

	kafkaAddr := env("KAFKA_ADDR", "localhost:9092")
	c, err := net.Dial("tcp", kafkaAddr)
	if err == nil {
		kafkaIsAvailable = true
		c.Close()
	}

	kafkaShouldBeAvailable = (env("CI", "") != "")
}

func checkKafkaAvailability(t *testing.T) {
	if !kafkaIsAvailable {
		if kafkaShouldBeAvailable {
			t.Fatal("Kafka broker is not available")
		} else {
			t.Skip("Kafka broker is not available")
		}
	}
}

func TestProducingMessages(t *testing.T) {
	checkKafkaAvailability(t)

	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	consumerConfig := NewConsumerConfig()
	consumerConfig.OffsetMethod = OffsetMethodNewest

	consumer, err := NewConsumer(client, "single_partition", 0, "functional_test", consumerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	producer, err := NewProducer(client, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	for i := 1; i <= TestBatchSize; i++ {
		err = producer.SendMessage("single_partition", nil, StringEncoder(fmt.Sprintf("testing %d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	events := consumer.Events()
	for i := 1; i <= TestBatchSize; i++ {
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("Not received any more events in the last 10 seconds.")

		case event := <-events:
			if string(event.Value) != fmt.Sprintf("testing %d", i) {
				t.Fatal("Unexpected message with index %d: %s", i, event.Value)
			}
		}

	}
}

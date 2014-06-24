package sarama

import (
	"fmt"
	"testing"
	"time"
)

const (
	TestBatchSize = 1000
)

func TestProducingMessages(t *testing.T) {
	client, err := NewClient("functional_test", []string{"localhost:9092"}, nil)
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

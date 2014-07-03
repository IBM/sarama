package sarama

import (
	"fmt"
	"testing"
)

const TestMessage = "ABC THE MESSAGE"

func TestDefaultProducerConfigValidates(t *testing.T) {
	config := NewProducerConfig()
	if err := config.Validate(); err != nil {
		t.Error(err)
	}
}

func TestSimpleProducer(t *testing.T) {
	broker1 := NewMockBroker(t, 1)
	broker2 := NewMockBroker(t, 2)
	defer broker1.Close()
	defer broker2.Close()

	response1 := new(MetadataResponse)
	response1.AddBroker(broker2.Addr(), broker2.BrokerID())
	response1.AddTopicPartition("my_topic", 0, 2)
	broker1.Returns(response1)

	response2 := new(ProduceResponse)
	response2.AddTopicPartition("my_topic", 0, NoError)
	for i := 0; i < 10; i++ {
		broker2.Returns(response2)
	}

	client, err := NewClient("client_id", []string{broker1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewSimpleProducer(client, "my_topic", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	for i := 0; i < 10; i++ {
		err = producer.SendMessage(nil, StringEncoder(TestMessage))
		if err != nil {
			t.Error(err)
		}
	}
}

func TestProducer(t *testing.T) {
	broker1 := NewMockBroker(t, 1)
	broker2 := NewMockBroker(t, 2)
	defer broker1.Close()
	defer broker2.Close()

	response1 := new(MetadataResponse)
	response1.AddBroker(broker2.Addr(), broker2.BrokerID())
	response1.AddTopicPartition("my_topic", 0, 2)
	broker1.Returns(response1)

	response2 := new(ProduceResponse)
	response2.AddTopicPartition("my_topic", 0, NoError)
	broker2.Returns(response2)

	client, err := NewClient("client_id", []string{broker1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	config := NewProducerConfig()
	config.FlushMsgCount = 10
	config.AckSuccesses = true
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	for i := 0; i < 10; i++ {
		producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	for i := 0; i < 10; i++ {
		msg := <-producer.Errors()
		if msg.Err != nil {
			t.Error(err)
		}
	}
}

func TestProducerMultipleFlushes(t *testing.T) {
	t.Skip("TODO")
}

func TestProducerMultipleBrokers(t *testing.T) {
	t.Skip("TODO")
}

// Here we test that when two messages are sent in the same buffered request,
// and more messages are enqueued while the request is pending, everything
// happens correctly; that is, the first messages are retried before the next
// batch is allowed to submit.
func TestProducerFailureRetry(t *testing.T) {
	t.Skip("TODO")
}

func ExampleProducer() {
	client, err := NewClient("client_id", []string{"localhost:9092"}, NewClientConfig())
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> connected")
	}
	defer client.Close()

	producer, err := NewProducer(client, nil)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	for {
		select {
		case producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder("testing 123")}:
			fmt.Println("> message queued")
		case err := <-producer.Errors():
			panic(err)
		}
	}
}

func ExampleSimpleProducer() {
	client, err := NewClient("client_id", []string{"localhost:9092"}, NewClientConfig())
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> connected")
	}
	defer client.Close()

	producer, err := NewSimpleProducer(client, "my_topic", nil)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	for {
		err = producer.SendMessage(nil, StringEncoder("testing 123"))
		if err != nil {
			panic(err)
		} else {
			fmt.Println("> message sent")
		}
	}
}

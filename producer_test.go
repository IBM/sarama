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
	defer safeClose(t, client)

	producer, err := NewSimpleProducer(client, "my_topic", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, producer)

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
	response1.AddTopicPartition("my_topic", 0, broker2.BrokerID())
	broker1.Returns(response1)

	response2 := new(ProduceResponse)
	response2.AddTopicPartition("my_topic", 0, NoError)
	broker2.Returns(response2)

	client, err := NewClient("client_id", []string{broker1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, client)

	config := NewProducerConfig()
	config.FlushMsgCount = 10
	promise := make(chan error)
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, producer)

	for i := 0; i < 10; i++ {
		producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage), Promise: promise}
	}
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
		case err := <-promise:
			if err != nil {
				t.Error(err)
			}
		}
	}
}

func TestProducerMultipleFlushes(t *testing.T) {
	broker1 := NewMockBroker(t, 1)
	broker2 := NewMockBroker(t, 2)
	defer broker1.Close()
	defer broker2.Close()

	response1 := new(MetadataResponse)
	response1.AddBroker(broker2.Addr(), broker2.BrokerID())
	response1.AddTopicPartition("my_topic", 0, broker2.BrokerID())
	broker1.Returns(response1)

	response2 := new(ProduceResponse)
	response2.AddTopicPartition("my_topic", 0, NoError)
	broker2.Returns(response2)
	broker2.Returns(response2)
	broker2.Returns(response2)

	client, err := NewClient("client_id", []string{broker1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, client)

	config := NewProducerConfig()
	config.FlushMsgCount = 5
	promise := make(chan error)
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	for flush := 0; flush < 3; flush++ {
		for i := 0; i < 5; i++ {
			producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage), Promise: promise}
		}
		for i := 0; i < 5; i++ {
			select {
			case msg := <-producer.Errors():
				t.Error(msg.Err)
			case err := <-promise:
				if err != nil {
					t.Error(err)
				}
			}
		}
	}
}

func TestProducerMultipleBrokers(t *testing.T) {
	broker1 := NewMockBroker(t, 1)
	broker2 := NewMockBroker(t, 2)
	broker3 := NewMockBroker(t, 3)
	defer broker1.Close()
	defer broker2.Close()
	defer broker3.Close()

	response1 := new(MetadataResponse)
	response1.AddBroker(broker2.Addr(), broker2.BrokerID())
	response1.AddBroker(broker3.Addr(), broker3.BrokerID())
	response1.AddTopicPartition("my_topic", 0, broker2.BrokerID())
	response1.AddTopicPartition("my_topic", 1, broker3.BrokerID())
	broker1.Returns(response1)

	response2 := new(ProduceResponse)
	response2.AddTopicPartition("my_topic", 0, NoError)
	broker2.Returns(response2)

	response3 := new(ProduceResponse)
	response3.AddTopicPartition("my_topic", 1, NoError)
	broker3.Returns(response3)

	client, err := NewClient("client_id", []string{broker1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, client)

	config := NewProducerConfig()
	config.FlushMsgCount = 5
	promise := make(chan error)
	config.Partitioner = NewRoundRobinPartitioner
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, producer)

	for i := 0; i < 10; i++ {
		producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage), Promise: promise}
	}
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
		case err := <-promise:
			if err != nil {
				t.Error(err)
			}
		}
	}
}

func TestProducerFailureRetry(t *testing.T) {
	broker1 := NewMockBroker(t, 1)
	broker2 := NewMockBroker(t, 2)
	broker3 := NewMockBroker(t, 3)

	response1 := new(MetadataResponse)
	response1.AddBroker(broker2.Addr(), broker2.BrokerID())
	response1.AddTopicPartition("my_topic", 0, broker2.BrokerID())
	broker1.Returns(response1)

	client, err := NewClient("client_id", []string{broker1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	config := NewProducerConfig()
	config.FlushMsgCount = 10
	promise := make(chan error)
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}
	broker1.Close()

	for i := 0; i < 10; i++ {
		producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage), Promise: promise}
	}
	response2 := new(ProduceResponse)
	response2.AddTopicPartition("my_topic", 0, NotLeaderForPartition)
	broker2.Returns(response2)

	response3 := new(MetadataResponse)
	response3.AddBroker(broker3.Addr(), broker3.BrokerID())
	response3.AddTopicPartition("my_topic", 0, broker3.BrokerID())
	broker2.Returns(response3)

	response4 := new(ProduceResponse)
	response4.AddTopicPartition("my_topic", 0, NoError)
	broker3.Returns(response4)
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
		case err := <-promise:
			if err != nil {
				t.Error(err)
			}
		}
	}
	broker2.Close()

	for i := 0; i < 10; i++ {
		producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage), Promise: promise}
	}
	broker3.Returns(response4)
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
		case err := <-promise:
			if err != nil {
				t.Error(err)
			}
		}
	}

	broker3.Close()
	safeClose(t, producer)
	safeClose(t, client)
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
			panic(err.Err)
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

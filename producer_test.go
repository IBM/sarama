package sarama

import (
	"fmt"
	"github.com/Shopify/sarama/mockbroker"
	"testing"
)

func TestSimpleProducer(t *testing.T) {

	mb1 := mockbroker.New(t, 1)
	mb2 := mockbroker.New(t, 2)
	defer mb1.Close()
	defer mb2.Close()

	mb1.ExpectMetadataRequest().
		AddBroker(mb2).
		AddTopicPartition("my_topic", 0, 2)

	// TODO: While the third parameter is the number of messages to expect,
	// really nothing about the message is actually asserted by the mock. This is
	// a problem for the future.
	for i := 0; i < 10; i++ {
		mb2.ExpectProduceRequest().
			AddTopicPartition("my_topic", 0, 1, nil)
	}

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	producer, err := NewProducer(client, "my_topic", &ProducerConfig{RequiredAcks: WaitForLocal})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	for i := 0; i < 10; i++ {
		err = producer.SendMessage(nil, StringEncoder("ABC THE MESSAGE"))
		if err != nil {
			t.Error(err)
		}
	}
}

func ExampleProducer() {
	client, err := NewClient("my_client", []string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> connected")
	}
	defer client.Close()

	producer, err := NewProducer(client, "my_topic", &ProducerConfig{RequiredAcks: WaitForLocal})
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	err = producer.SendMessage(nil, StringEncoder("testing 123"))
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> message sent")
	}
}

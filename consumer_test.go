package sarama

import (
	"fmt"
	"testing"
	"time"
)

var (
	consumerStopper = []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}
	extraBrokerMetadata = []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}
)

func TestSimpleConsumer(t *testing.T) {

	mb1 := NewMockBroker(t, 1)
	mb2 := NewMockBroker(t, 2)

	mdr := new(MetadataResponse)
	mdr.AddBroker(mb2.Addr(), int32(mb2.BrokerID()))
	mdr.AddTopicPartition("my_topic", 0, 2)
	mb2.Returns(mdr)

	for i := 0; i < 10; i++ {
		fr := new(FetchResponse)
		fr.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(i))
		mb2.Returns(fr)
	}

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	consumer, err := NewConsumer(client, "my_topic", 0, "my_consumer_group", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	defer mb1.Close()
	defer mb2.Close()

	for i := 0; i < 10; i++ {
		event := <-consumer.Events()
		if event.Err != nil {
			t.Error(err)
		}
		if event.Offset != int64(i) {
			t.Error("Incorrect message offset!")
		}
	}

}

func TestConsumerRawOffset(t *testing.T) {

	mb1 := NewMockBroker(t, 1)
	mb2 := NewMockBroker(t, 2)

	mdr := new(MetadataResponse)
	mdr.AddBroker(mb2.Addr(), int32(mb2.BrokerID()))
	mdr.AddTopicPartition("my_topic", 0, 2)
	mb2.Returns(mdr)

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	consumer, err := NewConsumer(client, "my_topic", 0, "my_consumer_group", &ConsumerConfig{OffsetMethod: OffsetMethodManual, OffsetValue: 1234})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	defer mb1.Close()
	defer mb2.Close()

	if consumer.offset != 1234 {
		t.Error("Raw offset not set correctly")
	}
}

func TestConsumerLatestOffset(t *testing.T) {

	mb1 := NewMockBroker(t, 1)
	mb2 := NewMockBroker(t, 2)

	mdr := new(MetadataResponse)
	mdr.AddBroker(mb2.Addr(), int32(mb2.BrokerID()))
	mdr.AddTopicPartition("my_topic", 0, 2)
	mb2.Returns(mdr)

	ofr := new(OffsetFetchResponse)
	ofr.AddTopicPartition("my_topic", 0, 0x010101)
	mb2.Returns(ofr)

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	consumer, err := NewConsumer(client, "my_topic", 0, "my_consumer_group", &ConsumerConfig{OffsetMethod: OffsetMethodNewest})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	defer mb2.Close()
	defer mb1.Close()

	if consumer.offset != 0x010101 {
		t.Error("Latest offset not fetched correctly")
	}
}

func ExampleConsumer() {
	client, err := NewClient("my_client", []string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> connected")
	}
	defer client.Close()

	consumer, err := NewConsumer(client, "my_topic", 0, "my_consumer_group", nil)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> consumer ready")
	}
	defer consumer.Close()

	msgCount := 0
consumerLoop:
	for {
		select {
		case event := <-consumer.Events():
			if event.Err != nil {
				panic(event.Err)
			}
			msgCount += 1
		case <-time.After(5 * time.Second):
			fmt.Println("> timed out")
			break consumerLoop
		}
	}
	fmt.Println("Got", msgCount, "messages.")
}

package sarama

import (
	"encoding/binary"
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
	masterResponses := make(chan []byte, 1)
	extraResponses := make(chan []byte)
	mockBroker := NewMockBroker(t, masterResponses)
	mockExtra := NewMockBroker(t, extraResponses)
	defer mockBroker.Close()
	defer mockExtra.Close()

	// return the extra mock as another available broker
	response := make([]byte, len(extraBrokerMetadata))
	copy(response, extraBrokerMetadata)
	binary.BigEndian.PutUint32(response[19:], uint32(mockExtra.Port()))
	masterResponses <- response
	go func() {
		for i := 0; i < 10; i++ {
			msg := []byte{
				0x00, 0x00, 0x00, 0x01,
				0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c',
				0x00, 0x00, 0x00, 0x01,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x1C,
				// messageSet
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x10,
				// message
				0x23, 0x96, 0x4a, 0xf7, // CRC
				0x00,
				0x00,
				0xFF, 0xFF, 0xFF, 0xFF,
				0x00, 0x00, 0x00, 0x02, 0x00, 0xEE}
			binary.BigEndian.PutUint64(msg[36:], uint64(i))
			extraResponses <- msg
		}
		extraResponses <- consumerStopper
	}()

	client, err := NewClient("client_id", []string{mockBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	consumer, err := NewConsumer(client, "my_topic", 0, "my_consumer_group", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

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
	masterResponses := make(chan []byte, 1)
	extraResponses := make(chan []byte, 1)
	mockBroker := NewMockBroker(t, masterResponses)
	mockExtra := NewMockBroker(t, extraResponses)
	defer mockBroker.Close()
	defer mockExtra.Close()

	// return the extra mock as another available broker
	response := make([]byte, len(extraBrokerMetadata))
	copy(response, extraBrokerMetadata)
	binary.BigEndian.PutUint32(response[19:], uint32(mockExtra.Port()))
	masterResponses <- response
	extraResponses <- consumerStopper

	client, err := NewClient("client_id", []string{mockBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	consumer, err := NewConsumer(client, "my_topic", 0, "my_consumer_group", &ConsumerConfig{OffsetMethod: RAW_OFFSET, OffsetValue: 1234})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	if consumer.offset != 1234 {
		t.Error("Raw offset not set correctly")
	}
}

func TestConsumerLatestOffset(t *testing.T) {
	masterResponses := make(chan []byte, 1)
	extraResponses := make(chan []byte, 2)
	mockBroker := NewMockBroker(t, masterResponses)
	mockExtra := NewMockBroker(t, extraResponses)
	defer mockBroker.Close()
	defer mockExtra.Close()

	// return the extra mock as another available broker
	response := make([]byte, len(extraBrokerMetadata))
	copy(response, extraBrokerMetadata)
	binary.BigEndian.PutUint32(response[19:], uint32(mockExtra.Port()))
	masterResponses <- response
	extraResponses <- []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x01,
	}
	extraResponses <- consumerStopper

	client, err := NewClient("client_id", []string{mockBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	consumer, err := NewConsumer(client, "my_topic", 0, "my_consumer_group", &ConsumerConfig{OffsetMethod: LATEST_OFFSET})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

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

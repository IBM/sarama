package sarama

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

func TestSimpleConsumer(t *testing.T) {
	masterResponses := make(chan []byte, 1)
	extraResponses := make(chan []byte)
	mockBroker := NewMockBroker(t, masterResponses)
	mockExtra := NewMockBroker(t, extraResponses)
	defer mockBroker.Close()
	defer mockExtra.Close()

	// return the extra mock as another available broker
	response := []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x07, 'm', 'y', 'T', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}
	binary.BigEndian.PutUint32(response[19:], uint32(mockExtra.Port()))
	masterResponses <- response
	go func() {
		for i := 0; i < 10; i++ {
			msg := []byte{
				0x00, 0x00, 0x00, 0x01,
				0x00, 0x07, 'm', 'y', 'T', 'o', 'p', 'i', 'c',
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
			binary.BigEndian.PutUint64(msg[35:], uint64(i))
			extraResponses <- msg
		}
		extraResponses <- []byte{
			0x00, 0x00, 0x00, 0x01,
			0x00, 0x07, 'm', 'y', 'T', 'o', 'p', 'i', 'c',
			0x00, 0x00, 0x00, 0x01,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00}
	}()

	client, err := NewClient("clientID", []string{mockBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	consumer, err := NewConsumer(client, "myTopic", 0, "myConsumerGroup", nil)
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

func ExampleConsumer() {
	client, err := NewClient("myClient", []string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> connected")
	}
	defer client.Close()

	consumer, err := NewConsumer(client, "myTopic", 0, "myConsumerGroup", nil)
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

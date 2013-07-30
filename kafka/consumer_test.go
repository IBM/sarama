package kafka

import (
	"encoding/binary"
	"fmt"
	"sarama/mock"
	"testing"
	"time"
)

func TestSimpleConsumer(t *testing.T) {
	masterResponses := make(chan []byte, 1)
	extraResponses := make(chan []byte)
	mockBroker := mock.NewBroker(t, masterResponses)
	mockExtra := mock.NewBroker(t, extraResponses)
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

	client, err := NewClient("clientID", "localhost", mockBroker.Port())
	if err != nil {
		t.Fatal(err)
	}

	consumer, err := NewConsumer(client, "myTopic", 0, "myConsumerGroup")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		select {
		case msg := <-consumer.Messages():
			if msg.Offset != int64(i) {
				t.Error("Incorrect message offset!")
			}
		case err := <-consumer.Errors():
			t.Error(err)
		}
	}

	consumer.Close()
	client.Close()
}

func ExampleConsumer() {
	client, err := NewClient("myClient", "localhost", 9092)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> connected")
	}

	consumer, err := NewConsumer(client, "myTopic", 0, "myConsumerGroup")
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> consumer ready")
	}

consumerLoop:
	for {
		select {
		case msg := <-consumer.Messages():
			fmt.Println(msg)
		case err := <-consumer.Errors():
			panic(err)
		case <-time.After(5 * time.Second):
			fmt.Println("> timed out")
			break consumerLoop
		}
	}

	consumer.Close()
	client.Close()
}

package sarama

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestSimpleProducer(t *testing.T) {
	responses := make(chan []byte, 1)
	extraResponses := make(chan []byte)
	mockBroker := NewMockBroker(t, responses)
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
	responses <- response
	go func() {
		for i := 0; i < 10; i++ {
			msg := []byte{
				0x00, 0x00, 0x00, 0x01,
				0x00, 0x07, 'm', 'y', 'T', 'o', 'p', 'i', 'c',
				0x00, 0x00, 0x00, 0x01,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
			binary.BigEndian.PutUint64(msg[23:], uint64(i))
			extraResponses <- msg
		}
	}()

	client, err := NewClient("clientID", "localhost", mockBroker.Port())
	if err != nil {
		t.Fatal(err)
	}

	producer := NewProducer(client, "myTopic", &RandomPartitioner{})
	for i := 0; i < 10; i++ {
		err = producer.SendMessage(nil, StringEncoder("ABC THE MESSAGE"))
		if err != nil {
			t.Error(err)
		}
	}

	client.Close()
}

func ExampleProducer() {
	client, err := NewClient("myClient", "localhost", 9092)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> connected")
	}
	producer := NewProducer(client, "myTopic", RandomPartitioner{})

	err = producer.SendMessage(nil, StringEncoder("testing 123"))
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> message sent")
	}

	client.Close()
}

package sarama

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

func TestSimpleMultiProducer(t *testing.T) {
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
		0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}
	binary.BigEndian.PutUint32(response[19:], uint32(mockExtra.Port()))
	responses <- response
	go func() {
		msg := []byte{
			0x00, 0x00, 0x00, 0x01,
			0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c',
			0x00, 0x00, 0x00, 0x01,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
		binary.BigEndian.PutUint64(msg[23:], 0)
		extraResponses <- msg
	}()

	client, err := NewClient("client_id", []string{mockBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewMultiProducer(client, &MultiProducerConfig{
		RequiredAcks:  WaitForLocal,
		MaxBufferTime: 1000000, // "never"
		// So that we flush once, after the 10th message.
		MaxBufferBytes: uint32((len("ABC THE MESSAGE") * 10) - 1),
	})
	defer producer.Close()

	for i := 0; i < 10; i++ {
		err = producer.SendMessage("my_topic", nil, StringEncoder("ABC THE MESSAGE"))
		if err != nil {
			t.Error(err)
		}
	}

	select {
	case err = <-producer.Errors():
		if err != nil {
			t.Error(err)
		}
	case <-time.After(1 * time.Second):
		t.Error(fmt.Errorf("Message was never received"))
	}

	select {
	case <-producer.Errors():
		t.Error(fmt.Errorf("too many values returned"))
	default:
	}

	// TODO: This doesn't really test that we ONLY flush once.
	// For example, change the MaxBufferBytes to be much lower.
}

func TestMultipleMultiProducer(t *testing.T) {

	// TODO: Submit events to 3 different topics on 2 different brokers.
	// Need to figure out how the request format works to return the broker
	// info for those two new brokers, and how to return multiple blocks in
	// a ProduceRespose

}

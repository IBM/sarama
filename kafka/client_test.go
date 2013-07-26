package kafka

import (
	"encoding/binary"
	"sarama/mock"
	"testing"
)

func TestSimpleClient(t *testing.T) {
	responses := make(chan []byte, 1)
	mockBroker := mock.NewBroker(t, responses)
	defer mockBroker.Close()

	// Only one response needed, an empty metadata response
	responses <- []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

	client, err := NewClient("clientID", "localhost", mockBroker.Port())
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
}

func TestExtraBroker(t *testing.T) {
	responses := make(chan []byte, 1)
	mockBroker := mock.NewBroker(t, responses)
	mockExtra := mock.NewBroker(t, make(chan []byte))
	defer mockBroker.Close()
	defer mockExtra.Close()

	// return the extra mock as another available broker
	response := []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}
	binary.BigEndian.PutUint32(response[19:], uint32(mockExtra.Port()))
	responses <- response

	client, err := NewClient("clientID", "localhost", mockBroker.Port())
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
}

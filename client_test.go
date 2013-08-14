package sarama

import (
	"encoding/binary"
	"testing"
)

func TestSimpleClient(t *testing.T) {
	responses := make(chan []byte, 1)
	mockBroker := NewMockBroker(t, responses)
	defer mockBroker.Close()

	// Only one response needed, an empty metadata response
	responses <- []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

	client, err := NewClient("clientID", "localhost", mockBroker.Port(), nil)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
}

func TestClientExtraBrokers(t *testing.T) {
	responses := make(chan []byte, 1)
	mockBroker := NewMockBroker(t, responses)
	mockExtra := NewMockBroker(t, make(chan []byte))
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

	client, err := NewClient("clientID", "localhost", mockBroker.Port(), nil)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
}

func TestClientMetadata(t *testing.T) {
	responses := make(chan []byte, 1)
	mockBroker := NewMockBroker(t, responses)
	mockExtra := NewMockBroker(t, make(chan []byte))
	defer mockBroker.Close()
	defer mockExtra.Close()

	// return the extra mock as another available broker
	response := []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x05,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x00,

		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x07, 'm', 'y', 'T', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x05,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}
	binary.BigEndian.PutUint32(response[19:], uint32(mockExtra.Port()))
	responses <- response

	client, err := NewClient("clientID", "localhost", mockBroker.Port(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	parts, err := client.partitions("myTopic")
	if err != nil {
		t.Error(err)
	} else if len(parts) != 1 || parts[0] != 0 {
		t.Error("Client returned incorrect partitions for myTopic:", parts)
	}

	tst, err := client.leader("myTopic", 0)
	if err != nil {
		t.Error(err)
	} else if tst.ID() != 5 {
		t.Error("Leader for myTopic had incorrect ID.")
	}
}

func TestClientRefreshBehaviour(t *testing.T) {
	responses := make(chan []byte, 3)
	mockBroker := NewMockBroker(t, responses)
	mockExtra := NewMockBroker(t, make(chan []byte))
	defer mockBroker.Close()
	defer mockExtra.Close()

	// return the extra mock as another available broker
	response := []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0xaa,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}
	binary.BigEndian.PutUint32(response[19:], uint32(mockExtra.Port()))
	responses <- response
	responses <- []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x07, 'm', 'y', 'T', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x05,
		0x00, 0x00, 0x00, 0x0e,
		0xFF, 0xFF, 0xFF, 0xFF,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}
	responses <- []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x07, 'm', 'y', 'T', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x0b,
		0x00, 0x00, 0x00, 0xaa,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}

	client, err := NewClient("clientID", "localhost", mockBroker.Port(), &ClientConfig{MetadataRetries: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	parts, err := client.partitions("myTopic")
	if err != nil {
		t.Error(err)
	} else if len(parts) != 1 || parts[0] != 0xb {
		t.Error("Client returned incorrect partitions for myTopic:", parts)
	}

	tst, err := client.leader("myTopic", 0xb)
	if err != nil {
		t.Error(err)
	} else if tst.ID() != 0xaa {
		t.Error("Leader for myTopic had incorrect ID.")
	}

	client.disconnectBroker(tst)
}

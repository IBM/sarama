package kafka

import "testing"
import "sarama/mock"

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

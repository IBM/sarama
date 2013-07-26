package mock

import "testing"

func ExampleBroker(t *testing.T) {
	responses := make(chan []byte)
	broker := NewBroker(t, responses)
	defer broker.Close()
}

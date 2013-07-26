package mock

import "testing"

func ExampleBroker(t *testing.T) {
	responses := make(chan []byte)
	broker, err := NewBroker(t, responses)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
}

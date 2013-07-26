package protocol

import (
	"fmt"
	"sarama/mock"
	"sarama/types"
	"testing"
)

func ExampleBroker() error {
	broker := NewBroker("localhost", 9092)
	err := broker.Connect()
	if err != nil {
		return err
	}

	request := MetadataRequest{Topics: []string{"myTopic"}}
	response, err := broker.GetMetadata("myClient", &request)

	fmt.Println("There are", len(response.Topics), "topics active in the cluster.")

	broker.Close()

	return nil
}

func TestBrokerEquals(t *testing.T) {
	var b1, b2 *Broker

	b1 = nil
	b2 = nil

	if !b1.Equals(b2) {
		t.Error("Two nil brokers didn't compare equal.")
	}

	b1 = NewBroker("abc", 123)

	if b1.Equals(b2) {
		t.Error("Non-nil and nil brokers compared equal.")
	}
	if b2.Equals(b1) {
		t.Error("Nil and non-nil brokers compared equal.")
	}

	b2 = NewBroker("abc", 1234)
	if b1.Equals(b2) || b2.Equals(b1) {
		t.Error("Brokers with different ports compared equal.")
	}

	b2 = NewBroker("abcd", 123)
	if b1.Equals(b2) || b2.Equals(b1) {
		t.Error("Brokers with different hosts compared equal.")
	}

	b2 = NewBroker("abc", 123)
	b2.id = -2
	if b1.Equals(b2) || b2.Equals(b1) {
		t.Error("Brokers with different ids compared equal.")
	}

	b2.id = -1
	if !b1.Equals(b2) || !b2.Equals(b1) {
		t.Error("Similar brokers did not compare equal.")
	}
}

func TestBrokerID(t *testing.T) {

	broker := NewBroker("abc", 123)

	if broker.ID() != -1 {
		t.Error("New broker didn't have an ID of -1.")
	}

	broker.id = 34
	if broker.ID() != 34 {
		t.Error("Manually setting broker ID did not take effect.")
	}
}

func TestSimpleBrokerCommunication(t *testing.T) {
	responses := make(chan []byte)
	mockBroker, err := mock.NewBroker(t, responses)
	if err != nil {
		t.Fatal(err)
	}
	defer mockBroker.Close()

	broker := NewBroker("localhost", mockBroker.Port())
	err = broker.Connect()
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for _, tt := range brokerTestTable {
			responses <- tt.response
		}
	}()
	for _, tt := range brokerTestTable {
		tt.runner(t, broker)
	}

	err = broker.Close()
	if err != nil {
		t.Error(err)
	}
}

// We're not testing encoding/decoding here, so most of the requests/responses will be empty for simplicity's sake
var brokerTestTable = []struct {
	response []byte
	runner   func(*testing.T, *Broker)
}{
	{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := MetadataRequest{}
			response, err := broker.GetMetadata("clientID", &request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("Metadata request got no response!")
			}
		}},

	{nil,
		func(t *testing.T, broker *Broker) {
			request := ProduceRequest{}
			request.RequiredAcks = types.NO_RESPONSE
			response, err := broker.Produce("clientID", &request)
			if err != nil {
				t.Error(err)
			}
			if response != nil {
				t.Error("Produce request with NO_RESPONSE got a response!")
			}
		}},

	{[]byte{0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := ProduceRequest{}
			request.RequiredAcks = types.WAIT_FOR_LOCAL
			response, err := broker.Produce("clientID", &request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("Produce request without NO_RESPONSE got no response!")
			}
		}},

	{[]byte{0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := FetchRequest{}
			response, err := broker.Fetch("clientID", &request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("Fetch request got no response!")
			}
		}},

	{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := OffsetFetchRequest{}
			response, err := broker.FetchOffset("clientID", &request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("OffsetFetch request got no response!")
			}
		}},

	{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := OffsetCommitRequest{}
			response, err := broker.CommitOffset("clientID", &request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("OffsetCommit request got no response!")
			}
		}},

	{[]byte{0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := OffsetRequest{}
			response, err := broker.GetAvailableOffsets("clientID", &request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("Offset request got no response!")
			}
		}},
}

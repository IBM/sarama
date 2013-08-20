package sarama

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

// MockBroker is a mock Kafka broker. It consists of a TCP server on a kernel-selected localhost port that
// accepts a single connection. It reads Kafka requests from that connection and returns each response
// from the channel provided at creation-time (if a response has a len of 0, nothing is sent, if a response
// the server sleeps for 250ms instead of reading a request).
//
// When running tests with one of these, it is strongly recommended to specify a timeout to `go test` so that if the broker hangs
// waiting for a response, the test panics.
//
// It is not necessary to prefix message length or correlation ID to your response bytes, the server does that
// automatically as a convenience.
type MockBroker struct {
	port      int32
	stopper   chan bool
	responses chan []byte
	listener  net.Listener
	t         *testing.T
}

func (b *MockBroker) Port() int32 {
	return b.port
}

func (b *MockBroker) Addr() string {
	return b.listener.Addr().String()
}

// Close closes the response channel originally provided, then waits to make sure
// that all requests/responses matched up before exiting.
func (b *MockBroker) Close() {
	close(b.responses)
	<-b.stopper
}

func (b *MockBroker) serverLoop() {
	defer close(b.stopper)
	conn, err := b.listener.Accept()
	if err != nil {
		b.t.Error(err)
		conn.Close()
		b.listener.Close()
		return
	}
	reqHeader := make([]byte, 4)
	resHeader := make([]byte, 8)
	for response := range b.responses {
		if response == nil {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		_, err := io.ReadFull(conn, reqHeader)
		if err != nil {
			b.t.Error(err)
			conn.Close()
			b.listener.Close()
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(reqHeader))
		if len(body) < 10 {
			b.t.Error("Kafka request too short.")
			conn.Close()
			b.listener.Close()
			return
		}
		_, err = io.ReadFull(conn, body)
		if err != nil {
			b.t.Error(err)
			conn.Close()
			b.listener.Close()
			return
		}
		if len(response) == 0 {
			continue
		}
		binary.BigEndian.PutUint32(resHeader, uint32(len(response)+4))
		binary.BigEndian.PutUint32(resHeader[4:], binary.BigEndian.Uint32(body[4:]))
		_, err = conn.Write(resHeader)
		if err != nil {
			b.t.Error(err)
			conn.Close()
			b.listener.Close()
			return
		}
		_, err = conn.Write(response)
		if err != nil {
			b.t.Error(err)
			conn.Close()
			b.listener.Close()
			return
		}
	}
	err = conn.Close()
	if err != nil {
		b.t.Error(err)
		b.listener.Close()
		return
	}
	err = b.listener.Close()
	if err != nil {
		b.t.Error(err)
		return
	}
}

// NewMockBroker launches a fake Kafka broker. It takes a testing.T as provided by the test framework and a channel of responses to use.
// If an error occurs it is simply logged to the testing.T and the broker exits.
func NewMockBroker(t *testing.T, responses chan []byte) *MockBroker {
	var err error

	broker := new(MockBroker)
	broker.stopper = make(chan bool)
	broker.responses = responses
	broker.t = t

	broker.listener, err = net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	_, portStr, err := net.SplitHostPort(broker.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := strconv.ParseInt(portStr, 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	broker.port = int32(tmp)

	go broker.serverLoop()

	return broker
}

func ExampleBroker() error {
	broker := NewBroker("localhost:9092")
	err := broker.Open()
	if err != nil {
		return err
	}
	defer broker.Close()

	request := MetadataRequest{Topics: []string{"myTopic"}}
	response, err := broker.GetMetadata("myClient", &request)
	if err != nil {
		return err
	}

	fmt.Println("There are", len(response.Topics), "topics active in the cluster.")

	return nil
}

func TestBrokerEquals(t *testing.T) {
	var b1, b2 *Broker

	b1 = nil
	b2 = nil

	if !b1.Equals(b2) {
		t.Error("Two nil brokers didn't compare equal.")
	}

	b1 = NewBroker("abc:123")

	if b1.Equals(b2) {
		t.Error("Non-nil and nil brokers compared equal.")
	}
	if b2.Equals(b1) {
		t.Error("Nil and non-nil brokers compared equal.")
	}

	b2 = NewBroker("abc:1234")
	if b1.Equals(b2) || b2.Equals(b1) {
		t.Error("Brokers with different addrs compared equal.")
	}

	b2 = NewBroker("abc:123")
	b2.id = -2
	if b1.Equals(b2) || b2.Equals(b1) {
		t.Error("Brokers with different ids compared equal.")
	}

	b2.id = -1
	if !b1.Equals(b2) || !b2.Equals(b1) {
		t.Error("Similar brokers did not compare equal.")
	}
}

func TestBrokerAccessors(t *testing.T) {

	broker := NewBroker("abc:123")

	if broker.ID() != -1 {
		t.Error("New broker didn't have an ID of -1.")
	}

	if broker.Addr() != "abc:123" {
		t.Error("New broker didn't have the correct address")
	}

	broker.id = 34
	if broker.ID() != 34 {
		t.Error("Manually setting broker ID did not take effect.")
	}
}

func TestSimpleBrokerCommunication(t *testing.T) {
	responses := make(chan []byte)
	mockBroker := NewMockBroker(t, responses)
	defer mockBroker.Close()

	broker := NewBroker(mockBroker.Addr())
	err := broker.Open()
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

	{[]byte{},
		func(t *testing.T, broker *Broker) {
			request := ProduceRequest{}
			request.RequiredAcks = NO_RESPONSE
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
			request.RequiredAcks = WAIT_FOR_LOCAL
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

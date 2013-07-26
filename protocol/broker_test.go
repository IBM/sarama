package protocol

import (
	"encoding/binary"
	"io"
	"net"
	"sarama/types"
	"strconv"
	"testing"
)

// FakeKafkaServer is a mock helper for testing the Broker and other higher-level APIs.
// It takes a testing.T as provided by the test framework and a channel of responses to use.
// It spawns a TCP server on a kernel-selected localhost port, then spawns a goroutine that reads Kafka requests
// from that port and returns each provided response in order (if a response is nil, nothing is sent).
// When the server is successfully created, it returns the port on which it is listening and a 'done' channel
// which it will close when it exits. Otherwise it will return an error (if an error occurs *in* the goroutine it
// is simply logged to the testing.T and the goroutine exits). There is also a StopFakeServer helper that leads to
// this recommended pattern in tests:
//
//	port, done, err := FakeKafkaServer(t, responses)
//	if err != nil {
//      	t.Fatal(err)
//      }
//      defer StopFakeServer(responses, done)
//
// When running tests like this, it is strongly recommended to specify a -timeout to `go test` so that if the test hangs
// waiting for a response, it automatically panics.
//
// It is not necessary to prefix message length or correlation ID to your response bytes, the server does that
// automatically as a convenience.
func FakeKafkaServer(t *testing.T, responses <-chan []byte) (int32, <-chan bool, error) {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return 0, nil, err
	}
	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return 0, nil, err
	}
	tmp, err := strconv.ParseInt(portStr, 10, 32)
	if err != nil {
		return 0, nil, err
	}
	port := int32(tmp)
	done := make(chan bool)
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			t.Error(err)
			conn.Close()
			ln.Close()
			return
		}
		reqHeader := make([]byte, 4)
		resHeader := make([]byte, 8)
		for response := range responses {
			_, err := io.ReadFull(conn, reqHeader)
			if err != nil {
				t.Error(err)
				conn.Close()
				ln.Close()
				return
			}
			body := make([]byte, binary.BigEndian.Uint32(reqHeader))
			if len(body) < 10 {
				t.Error("Kafka request too short.")
				conn.Close()
				ln.Close()
				return
			}
			_, err = io.ReadFull(conn, body)
			if err != nil {
				t.Error(err)
				conn.Close()
				ln.Close()
				return
			}
			if response == nil {
				continue
			}
			binary.BigEndian.PutUint32(resHeader, uint32(len(response)+4))
			binary.BigEndian.PutUint32(resHeader[4:], binary.BigEndian.Uint32(body[4:]))
			_, err = conn.Write(resHeader)
			if err != nil {
				t.Error(err)
				conn.Close()
				ln.Close()
				return
			}
			_, err = conn.Write(response)
			if err != nil {
				t.Error(err)
				conn.Close()
				ln.Close()
				return
			}
		}
		err = conn.Close()
		if err != nil {
			t.Error(err)
			ln.Close()
			return
		}
		err = ln.Close()
		if err != nil {
			t.Error(err)
			return
		}
	}()
	return port, done, nil
}

func StopFakeServer(responses chan []byte, done <-chan bool) {
	close(responses)
	<-done
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
	port, done, err := FakeKafkaServer(t, responses)
	if err != nil {
		t.Fatal(err)
	}
	defer StopFakeServer(responses, done)

	broker := NewBroker("localhost", port)
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

var brokerTestTable = []struct {
	response []byte
	runner   func(*testing.T, *Broker)
}{
	{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := MetadataRequest{}
			_, err := broker.GetMetadata("clientID", &request)
			if err != nil {
				t.Error(err)
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
}

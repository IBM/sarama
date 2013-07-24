package protocol

import (
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"testing"
)

// FakeKafkaServer is a mock helper for testing the Broker and other higher-level APIs.
// It takes a testing.T as provided by the test framework, a channel of responses, and a done channel.
// It spawns a TCP server on a kernel-selected localhost port, then spawns a goroutine that reads Kafka requests
// from that port and returns each provided response in order (if a response is nil, nothing is sent).
// It returns the port on which it is listening or an error (if an error is returned, the goroutine is not spawned,
// if an error occurs *in* the goroutine it is simply logged to the testing.T and the goroutine exits).
// When the responses channel closes, it closes the done channel and exits. The calling test must read from the done
// channel as its last step to ensure the number of requests and provided responses lined up correctly.
//
// When running tests with this, it is strongly recommended to specify a -timeout to `go test` so that if the test hangs
// waiting for a response, it automatically panics.
//
// It is not necessary to prefix message length or correlation ID to your response bytes, the server does that
// automatically as a convenience.
func FakeKafkaServer(t *testing.T, responses <-chan []byte, done chan<- bool) (int32, error) {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return 0, err
	}
	tmp, err := strconv.ParseInt(portStr, 10, 32)
	if err != nil {
		return 0, err
	}
	port := int32(tmp)
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
			binary.BigEndian.PutUint32(resHeader, uint32(len(response)))
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
	return port, nil
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

func TestBrokerConnectClose(t *testing.T) {
	done := make(chan bool)
	responses := make(chan []byte)
	port, err := FakeKafkaServer(t, responses, done)
	if err != nil {
		t.Error(err)
		return
	}
	broker := NewBroker("localhost", port)
	err = broker.Connect()
	if err != nil {
		t.Error(err)
		return
	}
	err = broker.Close()
	if err != nil {
		t.Error(err)
		return
	}
	close(responses)
	<-done
}

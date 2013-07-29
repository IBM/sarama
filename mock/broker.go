/*
Package mock defines some simple helper functions for mocking Kafka brokers.

It exists solely for testing other parts of the Sarama stack. It is in its own
package so that it can be imported by tests in multiple different packages.
*/
package mock

import (
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

// Broker is a mock Kafka broker. It consists of a TCP server on a kernel-selected localhost port that
// accepts a single connection. It reads Kafka requests from that connection and returns each response
// from the channel provided at creation-time (if a response has a len of 0, nothing is sent, if a response
// the server sleeps for 250ms instead of reading a request).
//
// When running tests with one of these, it is strongly recommended to specify a timeout to `go test` so that if the broker hangs
// waiting for a response, the test panics.
//
// It is not necessary to prefix message length or correlation ID to your response bytes, the server does that
// automatically as a convenience.
type Broker struct {
	port      int32
	stopper   chan bool
	responses chan []byte
	listener  net.Listener
	t         *testing.T
}

// Port is the kernel-select TCP port the broker is listening on.
func (b *Broker) Port() int32 {
	return b.port
}

// Close closes the response channel originally provided, then waits to make sure
// that all requests/responses matched up before exiting.
func (b *Broker) Close() {
	close(b.responses)
	<-b.stopper
}

func (b *Broker) serverLoop() {
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

// NewBroker launches a fake Kafka broker. It takes a testing.T as provided by the test framework and a channel of responses to use.
// If an error occurs it is simply logged to the testing.T and the broker exits.
func NewBroker(t *testing.T, responses chan []byte) *Broker {
	var err error

	broker := new(Broker)
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

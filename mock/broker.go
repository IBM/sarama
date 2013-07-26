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
)

// Broker is a mock Kafka broker. It consists of a TCP server on a kernel-selected localhost port that
// accepts a single connection. It reads Kafka requests from that connection and returns each response
// from the channel provided at creation-time (if a response is nil, nothing is sent).
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

// NewBroker launches a fake Kafka broker. It takes a testing.T as provided by the test framework and a channel of responses to use.
// If an error occurs in the broker it is simply logged to the testing.T and the broker exits.
func NewBroker(t *testing.T, responses chan []byte) (*Broker, error) {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}
	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return nil, err
	}
	tmp, err := strconv.ParseInt(portStr, 10, 32)
	if err != nil {
		return nil, err
	}
	broker := new(Broker)
	broker.port = int32(tmp)
	broker.stopper = make(chan bool)
	broker.responses = responses
	go func() {
		defer close(broker.stopper)
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
	return broker, nil
}

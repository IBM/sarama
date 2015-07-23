package sarama

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

// mockBroker is a mock Kafka broker. It consists of a TCP server on a kernel-selected localhost port that
// accepts a single connection. It reads Kafka requests from that connection and returns each response
// from the channel provided at creation-time (if a response has a len of 0, nothing is sent, if a response
// the server sleeps for 250ms instead of reading a request).
//
// When running tests with one of these, it is strongly recommended to specify a timeout to `go test` so that if the broker hangs
// waiting for a response, the test panics.
//
// It is not necessary to prefix message length or correlation ID to your response bytes, the server does that
// automatically as a convenience.
type mockBroker struct {
	brokerID     int32
	port         int32
	stopper      chan bool
	expectations chan encoder
	listener     net.Listener
	t            *testing.T
	latency      time.Duration
}

func (b *mockBroker) SetLatency(latency time.Duration) {
	b.latency = latency
}

func (b *mockBroker) BrokerID() int32 {
	return b.brokerID
}

func (b *mockBroker) Port() int32 {
	return b.port
}

func (b *mockBroker) Addr() string {
	return b.listener.Addr().String()
}

func (b *mockBroker) Close() {
	if len(b.expectations) > 0 {
		b.t.Errorf("Not all expectations were satisfied in mockBroker with ID=%d! Still waiting on %d", b.BrokerID(), len(b.expectations))
	}
	close(b.expectations)
	<-b.stopper
}

func (b *mockBroker) serverLoop() (ok bool) {
	var (
		err  error
		conn net.Conn
	)

	defer close(b.stopper)
	if conn, err = b.listener.Accept(); err != nil {
		return b.serverError(err, conn)
	}
	reqHeader := make([]byte, 4)
	resHeader := make([]byte, 8)
	for expectation := range b.expectations {
		_, err = io.ReadFull(conn, reqHeader)
		if err != nil {
			return b.serverError(err, conn)
		}
		body := make([]byte, binary.BigEndian.Uint32(reqHeader))
		if len(body) < 10 {
			return b.serverError(errors.New("Kafka request too short."), conn)
		}
		if _, err = io.ReadFull(conn, body); err != nil {
			return b.serverError(err, conn)
		}

		if b.latency > 0 {
			time.Sleep(b.latency)
		}

		response, err := encode(expectation)
		if err != nil {
			return false
		}
		if len(response) == 0 {
			continue
		}

		binary.BigEndian.PutUint32(resHeader, uint32(len(response)+4))
		binary.BigEndian.PutUint32(resHeader[4:], binary.BigEndian.Uint32(body[4:]))
		if _, err = conn.Write(resHeader); err != nil {
			return b.serverError(err, conn)
		}
		if _, err = conn.Write(response); err != nil {
			return b.serverError(err, conn)
		}
	}
	if err = conn.Close(); err != nil {
		return b.serverError(err, nil)
	}
	if err = b.listener.Close(); err != nil {
		b.t.Error(err)
		return false
	}
	return true
}

func (b *mockBroker) serverError(err error, conn net.Conn) bool {
	b.t.Error(err)
	if conn != nil {
		if err := conn.Close(); err != nil {
			b.t.Error(err)
		}
	}
	if err := b.listener.Close(); err != nil {
		b.t.Error(err)
	}
	return false
}

// newMockBroker launches a fake Kafka broker. It takes a *testing.T as provided by the
// test framework and a channel of responses to use.  If an error occurs it is
// simply logged to the *testing.T and the broker exits.
func newMockBroker(t *testing.T, brokerID int32) *mockBroker {
	return newMockBrokerAddr(t, brokerID, "localhost:0")
}

// newMockBrokerAddr behaves like newMockBroker but listens on the address you give
// it rather than just some ephemeral port.
func newMockBrokerAddr(t *testing.T, brokerID int32, addr string) *mockBroker {
	var err error

	broker := &mockBroker{
		stopper:      make(chan bool),
		t:            t,
		brokerID:     brokerID,
		expectations: make(chan encoder, 512),
	}

	broker.listener, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	Logger.Printf("mockbroker/%d listening on %s\n", brokerID, broker.listener.Addr().String())
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

func (b *mockBroker) Returns(e encoder) {
	b.expectations <- e
}

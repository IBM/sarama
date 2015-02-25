package sarama

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"time"
)

// TestState is a generic interface for a test state, implemented e.g. by testing.T
type TestState interface {
	Error(args ...interface{})
	Fatal(args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

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
	brokerID     int32
	port         int32
	stopper      chan bool
	expectations chan *BrokerExpectation
	listener     net.Listener
	t            TestState
}

type MockCluster map[int32]*MockBroker

type callback func()

type BrokerExpectation struct {
	Response       encoder
	Latency        time.Duration
	BeforeCallback callback
	AfterCallback  callback
}

func (b *MockBroker) BrokerID() int32 {
	return b.brokerID
}

func (b *MockBroker) Port() int32 {
	return b.port
}

func (b *MockBroker) Addr() string {
	return b.listener.Addr().String()
}

func (b *MockBroker) Close() {
	if len(b.expectations) > 0 {
		b.t.Errorf("Not all expectations were satisfied in mock broker with ID=%d! Still waiting on %d requests.", b.BrokerID(), len(b.expectations))
	}
	close(b.expectations)
	<-b.stopper
}

func (b *MockBroker) serverLoop() (ok bool) {
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

		if expectation.BeforeCallback != nil {
			expectation.BeforeCallback()
		}

		if expectation.Latency > 0 {
			time.Sleep(expectation.Latency)
		}

		response, err := encode(expectation.Response)
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

		if expectation.AfterCallback != nil {
			expectation.AfterCallback()
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

func (b *MockBroker) serverError(err error, conn net.Conn) bool {
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

// NewMockBroker launces a fake Kafka cluster consisting of a specified  number
// of brokers.
func NewMockCluster(t TestState, brokers int32) MockCluster {
	cluster := make(MockCluster)
	for i := int32(1); i <= brokers; i++ {
		cluster[i] = NewMockBroker(t, i)
	}
	return cluster
}

func (mc MockCluster) Addr() []string {
	addrs := make([]string, len(mc))
	for _, broker := range mc {
		addrs = append(addrs, broker.Addr())
	}
	return addrs
}

func (mc MockCluster) Close() {
	for _, broker := range mc {
		broker.Close()
	}
}

// NewMockBroker launches a fake Kafka broker. It takes a TestState (e.g. *testing.T) as provided by the
// test framework and a channel of responses to use.  If an error occurs it is
// simply logged to the TestState and the broker exits.
func NewMockBroker(t TestState, brokerID int32) *MockBroker {
	return NewMockBrokerAddr(t, brokerID, "localhost:0")
}

// NewMockBrokerAddr behaves like NewMockBroker but listens on the address you give
// it rather than just some ephemeral port.
func NewMockBrokerAddr(t TestState, brokerID int32, addr string) *MockBroker {
	var err error

	broker := &MockBroker{
		stopper:      make(chan bool),
		t:            t,
		brokerID:     brokerID,
		expectations: make(chan *BrokerExpectation, 512),
	}

	broker.listener, err = net.Listen("tcp", addr)
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

func (b *MockBroker) Returns(response encoder) {
	b.expectations <- &BrokerExpectation{Response: response}
}

func (b *MockBroker) Expects(expectation *BrokerExpectation) {
	b.expectations <- expectation
}

func (e *BrokerExpectation) WithLatency(latency time.Duration) *BrokerExpectation {
	e.Latency = latency
	return e
}

func (e *BrokerExpectation) Before(callback callback) *BrokerExpectation {
	e.BeforeCallback = callback
	return e
}

func (e *BrokerExpectation) After(callback callback) *BrokerExpectation {
	e.AfterCallback = callback
	return e
}

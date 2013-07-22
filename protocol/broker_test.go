package protocol

import (
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"testing"
)

func fakeTCPServer(t *testing.T, responses [][]byte, done chan<- bool) (int32, error) {
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
		conn, err := ln.Accept()
		if err != nil {
			t.Error(err)
			conn.Close()
			ln.Close()
			done<- true
			return
		}
		for _, response := range responses {
			header := make([]byte, 4)
			_, err := io.ReadFull(conn, header)
			if err != nil {
				t.Error(err)
				conn.Close()
				ln.Close()
				done<- true
				return
			}
			body := make([]byte, binary.BigEndian.Uint32(header))
			_, err = io.ReadFull(conn, body)
			if err != nil {
				t.Error(err)
				conn.Close()
				ln.Close()
				done<- true
				return
			}
			_, err = conn.Write(response)
			if err != nil {
				t.Error(err)
				conn.Close()
				ln.Close()
				done<- true
				return
			}
		}
		err = conn.Close()
		if err != nil {
			t.Error(err)
			ln.Close()
			done<- true
			return
		}
		err = ln.Close()
		if err != nil {
			t.Error(err)
			done<- true
			return
		}
		done<- true
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
	port, err := fakeTCPServer(t, [][]byte{}, done)
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
	<-done
}

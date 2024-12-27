//go:build !functional

package sarama

import (
	"errors"
	"fmt"
	"net"
	"testing"
)

func TestSentinelWithSingleWrappedError(t *testing.T) {
	t.Parallel()
	myNetError := &net.OpError{Op: "mock", Err: errors.New("op error")}
	error := Wrap(ErrOutOfBrokers, myNetError)

	expected := fmt.Sprintf("%s: %s", ErrOutOfBrokers, myNetError)
	actual := error.Error()
	if actual != expected {
		t.Errorf("unexpected value '%s' vs '%v'", expected, actual)
	}

	if !errors.Is(error, ErrOutOfBrokers) {
		t.Error("errors.Is unexpected result")
	}

	if !errors.Is(error, myNetError) {
		t.Error("errors.Is unexpected result")
	}

	var opError *net.OpError
	if !errors.As(error, &opError) {
		t.Error("errors.As unexpected result")
	} else if opError != myNetError {
		t.Error("errors.As wrong value")
	}

	unwrapped := errors.Unwrap(error)
	if errors.Is(unwrapped, ErrOutOfBrokers) || !errors.Is(unwrapped, myNetError) {
		t.Errorf("unexpected unwrapped value %v vs %vs", error, unwrapped)
	}
}

func TestSentinelWithMultipleWrappedErrors(t *testing.T) {
	t.Parallel()
	myNetError := &net.OpError{}
	myAddrError := &net.AddrError{}

	error := Wrap(ErrOutOfBrokers, myNetError, myAddrError)

	if !errors.Is(error, ErrOutOfBrokers) {
		t.Error("errors.Is unexpected result")
	}

	if !errors.Is(error, myNetError) {
		t.Error("errors.Is unexpected result")
	}

	if !errors.Is(error, myAddrError) {
		t.Error("errors.Is unexpected result")
	}

	unwrapped := errors.Unwrap(error)
	if errors.Is(unwrapped, ErrOutOfBrokers) || !errors.Is(unwrapped, myNetError) || !errors.Is(unwrapped, myAddrError) {
		t.Errorf("unwrapped value unexpected result")
	}
}

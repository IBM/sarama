package protocol

import (
	"bytes"
	"testing"
)

// no actual tests, just helper functions for testing structures that
// implement the encoder or decoder interfaces

func testEncodable(t *testing.T, name string, in encoder, result []byte) {
	packet, err := encode(in)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(packet, result) {
		t.Error("Encoding", name, "failed\ngot ", packet, "\nwant", result)
	}
}

package protocol

import (
	"bytes"
	"testing"
)

// no actual tests, just helper functions for testing structures that
// implement the encoder or decoder interfaces

func testEncodable(t *testing.T, name string, in encoder, expect []byte) {
	packet, err := encode(in)
	if err != nil {
		t.Error(err)
	} else if !bytes.Equal(packet, expect) {
		t.Error("Encoding", name, "failed\ngot ", packet, "\nwant", expect)
	}
}

func testDecodable(t *testing.T, name string, out decoder, in []byte) {
	err := decode(in, out)
	if err != nil {
		t.Error("Decoding", name, "failed:", err)
	}
}

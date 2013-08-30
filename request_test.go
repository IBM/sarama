package sarama

import (
	"bytes"
	"testing"
)

var (
	requestSimple = []byte{
		0x00, 0x00, 0x00, 0x17, // msglen
		0x06, 0x66,
		0x00, 0xD2,
		0x00, 0x00, 0x12, 0x34,
		0x00, 0x08, 'm', 'y', 'C', 'l', 'i', 'e', 'n', 't',
		0x00, 0x03, 'a', 'b', 'c'}
)

type testRequestBody struct {
}

func (s *testRequestBody) key() int16 {
	return 0x666
}

func (s *testRequestBody) version() int16 {
	return 0xD2
}

func (s *testRequestBody) encode(pe packetEncoder) error {
	return pe.putString("abc")
}

func TestRequest(t *testing.T) {
	request := request{correlationID: 0x1234, id: "myClient", body: new(testRequestBody)}
	testEncodable(t, "simple", &request, requestSimple)
}

// not specific to request tests, just helper functions for testing structures that
// implement the encoder or decoder interfaces that needed somewhere to live

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

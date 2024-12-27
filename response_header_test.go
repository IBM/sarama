//go:build !functional

package sarama

import "testing"

var (
	responseHeaderBytesV0 = []byte{
		0x00, 0x00, 0x0f, 0x00,
		0x0a, 0xbb, 0xcc, 0xff,
	}

	responseHeaderBytesV1 = []byte{
		0x00, 0x00, 0x0f, 0x00,
		0x0a, 0xbb, 0xcc, 0xff, 0x00,
	}
)

func TestResponseHeaderV0(t *testing.T) {
	header := responseHeader{}

	testVersionDecodable(t, "response header", &header, responseHeaderBytesV0, 0)
	if header.length != 0xf00 {
		t.Error("Decoding header length failed, got", header.length)
	}
	if header.correlationID != 0x0abbccff {
		t.Error("Decoding header correlation id failed, got", header.correlationID)
	}
}

func TestResponseHeaderV1(t *testing.T) {
	header := responseHeader{}

	testVersionDecodable(t, "response header", &header, responseHeaderBytesV1, 1)
	if header.length != 0xf00 {
		t.Error("Decoding header length failed, got", header.length)
	}
	if header.correlationID != 0x0abbccff {
		t.Error("Decoding header correlation id failed, got", header.correlationID)
	}
}

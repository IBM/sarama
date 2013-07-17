package protocol

import (
	"bytes"
	"testing"
)

var (
	fetchRequestNoBlocks = []byte{
		0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
)

func TestFetchRequestEncoding(t *testing.T) {
	request := new(FetchRequest)
	packet, err := encode(request)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(packet, fetchRequestNoBlocks) {
		t.Error("Encoding no blocks failed, got ", packet)
	}
}

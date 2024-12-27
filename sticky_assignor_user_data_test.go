//go:build !functional

package sarama

import (
	"encoding/base64"
	"testing"
)

func TestStickyAssignorUserDataV0(t *testing.T) {
	// Single topic with deterministic ordering across encode-decode
	req := &StickyAssignorUserDataV0{}
	data := decodeUserDataBytes(t, "AAAAAQADdDAzAAAAAQAAAAU=")
	testDecodable(t, "", req, data)
	testEncodable(t, "", req, data)

	// Multiple partitions
	req = &StickyAssignorUserDataV0{}
	data = decodeUserDataBytes(t, "AAAAAQADdDE4AAAAEgAAAAAAAAABAAAAAgAAAAMAAAAEAAAABQAAAAYAAAAHAAAACAAAAAkAAAAKAAAACwAAAAwAAAANAAAADgAAAA8AAAAQAAAAEQ==")
	testDecodable(t, "", req, data)

	// Multiple topics and partitions
	req = &StickyAssignorUserDataV0{}
	data = decodeUserDataBytes(t, "AAAABQADdDEyAAAAAgAAAAIAAAAKAAN0MTEAAAABAAAABAADdDE0AAAAAQAAAAgAA3QxMwAAAAEAAAANAAN0MDkAAAABAAAABQ==")
	testDecodable(t, "", req, data)
}

func TestStickyAssignorUserDataV1(t *testing.T) {
	// Single topic with deterministic ordering across encode-decode
	req := &StickyAssignorUserDataV1{}
	data := decodeUserDataBytes(t, "AAAAAQADdDA2AAAAAgAAAAAAAAAE/////w==")
	testDecodable(t, "", req, data)
	testEncodable(t, "", req, data)

	// Multiple topics and partitions
	req = &StickyAssignorUserDataV1{}
	data = decodeUserDataBytes(t, "AAAABgADdDEwAAAAAgAAAAIAAAAJAAN0MTIAAAACAAAAAwAAAAsAA3QxNAAAAAEAAAAEAAN0MTMAAAABAAAACwADdDE1AAAAAQAAAAwAA3QwOQAAAAEAAAAG/////w==")
	testDecodable(t, "", req, data)

	// Generation is populated
	req = &StickyAssignorUserDataV1{}
	data = decodeUserDataBytes(t, "AAAAAQAHdG9waWMwMQAAAAMAAAAAAAAAAQAAAAIAAAAB")
	testDecodable(t, "", req, data)
}

func decodeUserDataBytes(t *testing.T, base64Data string) []byte {
	data, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		t.Errorf("Error decoding data: %v", err)
		t.FailNow()
	}
	return data
}

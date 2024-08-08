package sarama

import (
	"testing"
)

func TestCooperativeStickyAssignorUserDataV0(t *testing.T) {
	req := &CooperativeStickyAssignorUserDataV0{}
	data := decodeUserDataBytes(t, "/////w==") // 0xff 0xff 0xff 0xff
	testDecodable(t, "", req, data)
	testEncodable(t, "", req, data)
}

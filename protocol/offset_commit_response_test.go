package protocol

import "testing"

var (
	emptyOffsetCommitResponse = []byte{
		0xFF, 0xFF,
		0x00, 0x00, 0x00, 0x00}

	normalOffsetCommitResponse = []byte{
		0x00, 0x02, 'a', 'z',
		0x00, 0x00, 0x00, 0x02,

		0x00, 0x01, 'm',
		0x00, 0x00, 0x00, 0x00,

		0x00, 0x01, 't',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x06}
)

func TestEmptyOffsetCommitResponse(t *testing.T) {
	response := OffsetCommitResponse{}

	testDecodable(t, "empty", &response, emptyOffsetCommitResponse)
	if response.ClientID != "" {
		t.Error("Decoding produced client ID where there was none.")
	}
	if len(response.Errors) != 0 {
		t.Error("Decoding produced errors where there were none.")
	}
}

func TestNormalOffsetCommitResponse(t *testing.T) {
	response := OffsetCommitResponse{}

	testDecodable(t, "normal", &response, normalOffsetCommitResponse)
	if response.ClientID != "az" {
		t.Error("Decoding produced wrong client ID.")
	}
	if len(response.Errors) == 2 {
		if len(response.Errors["m"]) != 0 {
			t.Error("Decoding produced errors for topic 'm' where there were none.")
		}
		if len(response.Errors["t"]) == 1 {
			if response.Errors["t"][0] != NOT_LEADER_FOR_PARTITION {
				t.Error("Decoding produced wrong error for topic 't' partition 0.")
			}
		} else {
			t.Error("Decoding produced wrong number of errors for topic 't'.")
		}
	} else {
		t.Error("Decoding produced wrong number of errors.")
	}
}

package sarama

import "testing"

var (
	emptyOffsetCommitResponse = []byte{
		0x00, 0x00, 0x00, 0x00}

	normalOffsetCommitResponse = []byte{
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

	if len(response.Errors) != 0 {
		t.Error("Decoding produced errors where there were none.")
	}
}

func TestNormalOffsetCommitResponse(t *testing.T) {
	response := OffsetCommitResponse{}

	testDecodable(t, "normal", &response, normalOffsetCommitResponse)

	if len(response.Errors) != 2 {
		t.Fatal("Decoding produced wrong number of errors.")
	}

	if len(response.Errors["m"]) != 0 {
		t.Error("Decoding produced errors for topic 'm' where there were none.")
	}

	if len(response.Errors["t"]) != 1 {
		t.Fatal("Decoding produced wrong number of errors for topic 't'.")
	}

	if response.Errors["t"][0] != ErrNotLeaderForPartition {
		t.Error("Decoding produced wrong error for topic 't' partition 0.")
	}

}

package sarama

import "testing"

var (
	emptyOffsetFetchResponse = []byte{
		0x00, 0x00, 0x00, 0x00}

	normalOffsetFetchResponse = []byte{
		0x00, 0x00, 0x00, 0x02,

		0x00, 0x01, 'm',
		0x00, 0x00, 0x00, 0x00,

		0x00, 0x01, 't',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x02, 'm', 'd',
		0x00, 0x07}
)

func TestEmptyOffsetFetchResponse(t *testing.T) {
	response := OffsetFetchResponse{}

	testDecodable(t, "empty", &response, emptyOffsetFetchResponse)

	if len(response.Blocks) != 0 {
		t.Error("Decoding produced topic blocks where there were none.")
	}
}

func TestNormalOffsetFetchResponse(t *testing.T) {
	response := OffsetFetchResponse{}

	testDecodable(t, "normal", &response, normalOffsetFetchResponse)

	if len(response.Blocks) != 2 {
		t.Fatal("Decoding produced wrong number of blocks.")
	}

	if len(response.Blocks["m"]) != 0 {
		t.Error("Decoding produced partitions for topic 'm' where there were none.")
	}

	if len(response.Blocks["t"]) != 1 {
		t.Fatal("Decoding produced wrong number of blocks for topic 't'.")
	}

	if response.Blocks["t"][0].Offset != 0 {
		t.Error("Decoding produced wrong offset for topic 't' partition 0.")
	}

	if response.Blocks["t"][0].Metadata != "md" {
		t.Error("Decoding produced wrong metadata for topic 't' partition 0.")
	}

	if response.Blocks["t"][0].Err != ErrRequestTimedOut {
		t.Error("Decoding produced wrong error for topic 't' partition 0.")
	}
}

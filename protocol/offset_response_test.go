package protocol

import "testing"
import "sarama/types"

var (
	emptyOffsetResponse = []byte{
		0x00, 0x00, 0x00, 0x00}

	normalOffsetResponse = []byte{
		0x00, 0x00, 0x00, 0x02,

		0x00, 0x01, 'a',
		0x00, 0x00, 0x00, 0x00,

		0x00, 0x01, 'z',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x02,
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06}
)

func TestEmptyOffsetResponse(t *testing.T) {
	response := OffsetResponse{}

	testDecodable(t, "empty", &response, emptyOffsetResponse)
	if len(response.Blocks) != 0 {
		t.Error("Decoding produced", len(response.Blocks), "topics where there were none.")
	}
}

func TestNormalOffsetResponse(t *testing.T) {
	response := OffsetResponse{}

	testDecodable(t, "normal", &response, normalOffsetResponse)
	if len(response.Blocks) == 2 {
		if len(response.Blocks["a"]) != 0 {
			t.Error("Decoding produced", len(response.Blocks["a"]), "partitions for topic 'a' where there were none.")
		}

		if len(response.Blocks["z"]) == 1 {
			if response.Blocks["z"][2].Err != types.NO_ERROR {
				t.Error("Decoding produced invalid error for topic z partition 2.")
			}
			if len(response.Blocks["z"][2].Offsets) == 2 {
				if response.Blocks["z"][2].Offsets[0] != 5 || response.Blocks["z"][2].Offsets[1] != 6 {
					t.Error("Decoding produced invalid offsets for topic z partition 2.")
				}
			} else {
				t.Error("Decoding produced invalid number of offsets for topic z partition 2.")
			}
		} else {
			t.Error("Decoding produced", len(response.Blocks["z"]), "partitions for topic 'z' where there was one.")
		}
	} else {
		t.Error("Decoding produced", len(response.Blocks), "topics where there were two.")
	}
}

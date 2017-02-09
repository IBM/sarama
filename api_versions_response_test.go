package sarama

import "testing"

var (
	apiVersionResponse = []byte{
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x03,
		0x00, 0x02,
		0x00, 0x01,
	}
)

func TestAPIVersionsResponse(t *testing.T) {
	var response *APIVersionsResponse

	response = new(APIVersionsResponse)
	testVersionDecodable(t, "no error", response, apiVersionResponse, 0)
	if response.Err != ErrNoError {
		t.Error("Decoding error failed: no error expected but found", response.Err)
	}
	if response.APIVersions[0].APIKey != 0x03 {
		t.Error("Decoding error: expected 0x03 but got", response.APIVersions[0].APIKey)
	}
	if response.APIVersions[0].MinVersion != 0x02 {
		t.Error("Decoding error: expected 0x02 but got", response.APIVersions[0].MinVersion)
	}
	if response.APIVersions[0].MaxVersion != 0x01 {
		t.Error("Decoding error: expected 0x01 but got", response.APIVersions[0].MaxVersion)
	}
}

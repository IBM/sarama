//go:build !functional

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

	apiVersionResponseV3 = []byte{
		0x00, 0x00, // no error
		0x02, // compact array length 1
		0x00, 0x03,
		0x00, 0x02,
		0x00, 0x01,
		0x00,                   // tagged fields
		0x00, 0x00, 0x00, 0x00, // throttle time
		0x01, 0x01, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // tagged fields (empty SupportedFeatures)
	}
)

func TestApiVersionsResponse(t *testing.T) {
	response := new(ApiVersionsResponse)
	testVersionDecodable(t, "no error", response, apiVersionResponse, 0)
	if response.ErrorCode != int16(ErrNoError) {
		t.Error("Decoding error failed: no error expected but found", response.ErrorCode)
	}
	if response.ApiKeys[0].ApiKey != 0x03 {
		t.Error("Decoding error: expected 0x03 but got", response.ApiKeys[0].ApiKey)
	}
	if response.ApiKeys[0].MinVersion != 0x02 {
		t.Error("Decoding error: expected 0x02 but got", response.ApiKeys[0].MinVersion)
	}
	if response.ApiKeys[0].MaxVersion != 0x01 {
		t.Error("Decoding error: expected 0x01 but got", response.ApiKeys[0].MaxVersion)
	}
}

func TestApiVersionsResponseV3(t *testing.T) {
	response := new(ApiVersionsResponse)
	response.Version = 3
	testVersionDecodable(t, "no error", response, apiVersionResponseV3, 3)
	if response.ErrorCode != int16(ErrNoError) {
		t.Error("Decoding error failed: no error expected but found", response.ErrorCode)
	}
	if response.ApiKeys[0].ApiKey != 0x03 {
		t.Error("Decoding error: expected 0x03 but got", response.ApiKeys[0].ApiKey)
	}
	if response.ApiKeys[0].MinVersion != 0x02 {
		t.Error("Decoding error: expected 0x02 but got", response.ApiKeys[0].MinVersion)
	}
	if response.ApiKeys[0].MaxVersion != 0x01 {
		t.Error("Decoding error: expected 0x01 but got", response.ApiKeys[0].MaxVersion)
	}
}

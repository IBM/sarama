package sarama

import "testing"

var (
	consumerMetadataResponseError = []byte{
		0x00, 0x0E,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}

	consumerMetadataResponseSuccess = []byte{
		0x00, 0x00,
		0x00, 0x00, 0x00, 0xAB,
		0x00, 0x03, 'f', 'o', 'o',
		0x00, 0x00, 0xCC, 0xDD}
)

func TestConsumerMetadataResponseError(t *testing.T) {
	response := ConsumerMetadataResponse{}

	testDecodable(t, "error", &response, consumerMetadataResponseError)

	if response.Err != OffsetsLoadInProgress {
		t.Error("Decoding produced incorrect error value.")
	}

	if response.CoordinatorId != 0 {
		t.Error("Decoding produced incorrect ID.")
	}

	if len(response.CoordinatorHost) != 0 {
		t.Error("Decoding produced incorrect host.")
	}

	if response.CoordinatorPort != 0 {
		t.Error("Decoding produced incorrect port.")
	}
}

func TestConsumerMetadataResponseSuccess(t *testing.T) {
	response := ConsumerMetadataResponse{}

	testDecodable(t, "success", &response, consumerMetadataResponseSuccess)

	if response.Err != NoError {
		t.Error("Decoding produced error value where there was none.")
	}

	if response.CoordinatorId != 0xAB {
		t.Error("Decoding produced incorrect coordinator ID.")
	}

	if response.CoordinatorHost != "foo" {
		t.Error("Decoding produced incorrect coordinator host.")
	}

	if response.CoordinatorPort != 0xCCDD {
		t.Error("Decoding produced incorrect coordinator port.")
	}
}

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

	if response.Err != ErrOffsetsLoadInProgress {
		t.Error("Decoding produced incorrect error value.")
	}

	if response.CoordinatorID != 0 {
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

	if response.Err != ErrNoError {
		t.Error("Decoding produced error value where there was none.")
	}

	if response.CoordinatorID != 0xAB {
		t.Error("Decoding produced incorrect coordinator ID.")
	}

	if response.CoordinatorHost != "foo" {
		t.Error("Decoding produced incorrect coordinator host.")
	}

	if response.CoordinatorPort != 0xCCDD {
		t.Error("Decoding produced incorrect coordinator port.")
	}

	if response.Coordinator.ID() != 0xAB {
		t.Error("Decoding produced incorrect coordinator ID.")
	}

	if response.Coordinator.Addr() != "foo:52445" {
		t.Error("Decoding produced incorrect coordinator address.")
	}
}

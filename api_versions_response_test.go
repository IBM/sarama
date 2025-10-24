//go:build !functional

package sarama

import (
	"testing"

	assert "github.com/stretchr/testify/require"
)

var (
	apiVersionResponseV0 = []byte{
		0x00, 0x00, // no error
		0x00, 0x00, 0x00, 0x04, // array length 4 (APIs)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // API Version Produce (v0-2)
		0x00, 0x01, 0x00, 0x00, 0x00, 0x03, // API Version Fetch (v0-3)
		0x00, 0x02, 0x00, 0x00, 0x00, 0x01, // API Version Offsets (v0-1)
		0x00, 0x03, 0x00, 0x00, 0x00, 0x02, // API Version Metadata (v0-2)
	}

	apiVersionResponseV1V2 = []byte{
		0x00, 0x00, // no error
		0x00, 0x00, 0x00, 0x05, // array length 5 (APIs)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // API Version Produce (v0-7)
		0x00, 0x01, 0x00, 0x00, 0x00, 0x0b, // API Version Fetch (v0-11)
		0x00, 0x02, 0x00, 0x00, 0x00, 0x05, // API Version Offsets (v0-5)
		0x00, 0x03, 0x00, 0x00, 0x00, 0x08, // API Version Metadata (v0-8)
		0x00, 0x04, 0x00, 0x00, 0x00, 0x02, // API Version LeaderAndIsr (v0-2)
		0x00, 0x00, 0x00, 0x40, // throttle time (64ms)
	}

	apiVersionResponseV3 = []byte{
		0x00, 0x00, // no error
		0x07,                               // compact array length 6 (APIs)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x08, // API Version Produce (v0-8)
		0x00,                               // empty tagged fields
		0x00, 0x01, 0x00, 0x00, 0x00, 0x0b, // API Version Fetch (v0-11)
		0x00,                               // empty tagged fields
		0x00, 0x02, 0x00, 0x00, 0x00, 0x05, // API Version Offsets (v0-5)
		0x00,                               // empty tagged fields
		0x00, 0x03, 0x00, 0x00, 0x00, 0x09, // API Version Metadata (v0-9)
		0x00,                               // empty tagged fields
		0x00, 0x04, 0x00, 0x00, 0x00, 0x04, // API Version LeaderAndIsr (v0-4)
		0x00,                               // empty tagged fields
		0x00, 0x05, 0x00, 0x00, 0x00, 0x02, // API Version StopReplica (v0-2)
		0x00,                   // empty tagged fields
		0x00, 0x00, 0x00, 0x80, // throttle time (128ms)
		0x00, // empty tagged fields
	}

	// unsupported version from kafka 0.10.2.1
	apiVersionsResponseUnsupportedVersionV0 = []byte{
		0x00, 0x23, // unsupported version error
		0x00, 0x00, 0x00, 0x00, // array length 0
	}

	// unsupported version from kafka 2.3.0
	apiVersionsResponseUnsupportedVersionV1V2 = []byte{
		0x00, 0x23, // unsupported version error
		0x00, 0x00, 0x00, 0x00, // array length 0
	}

	// unsupported version from kafka 2.4.0
	apiVersionsResponseUnsupportedVersionV3 = []byte{
		0x00, 0x23, // unsupported version error
		0x00, 0x00, 0x00, 0x01, // array length 1
		0x00, 0x12, 0x00, 0x00, 0x00, 0x03, // API Version ApiVersions (v0-3)
	}

	// unsupported version from kafka 4.1.0
	apiVersionsResponseUnsupportedVersionV4 = []byte{
		0x00, 0x23, // unsupported version error
		0x00, 0x00, 0x00, 0x01, // array length 1
		0x00, 0x12, 0x00, 0x00, 0x00, 0x04, // API Version ApiVersions (v0-4)
	}
)

func TestApiVersionsResponseV0(t *testing.T) {
	const v = 0
	response := new(ApiVersionsResponse)
	testVersionDecodable(t, "no error V0", response, apiVersionResponseV0, v)

	assert.Equal(t, int16(ErrNoError), response.ErrorCode)
	assert.Equal(t, []ApiVersionsResponseKey{
		{v, 0, 0, 2}, // API Version Produce (v0-2)
		{v, 1, 0, 3}, // API Version Fetch (v0-3)
		{v, 2, 0, 1}, // API Version Offsets (v0-1)
		{v, 3, 0, 2}, // API Version Metadata (v0-2)
	}, response.ApiKeys)
}

func TestApiVersionsResponseV1V2(t *testing.T) {
	response := new(ApiVersionsResponse)

	for _, v := range []int16{1, 2} {
		testVersionDecodable(t, "no error V1V2", response, apiVersionResponseV1V2, v)

		assert.Equal(t, int16(ErrNoError), response.ErrorCode)
		assert.Equal(t, []ApiVersionsResponseKey{
			{v, 0, 0, 7},  // API Version Produce (v0-7)
			{v, 1, 0, 11}, // API Version Fetch (v0-11)
			{v, 2, 0, 5},  // API Version Offsets (v0-5)
			{v, 3, 0, 8},  // API Version Metadata (v0-8)
			{v, 4, 0, 2},  // API Version LeaderAndIsr (v0-2)
		}, response.ApiKeys)
		assert.Equal(t, int32(64), response.ThrottleTimeMs)
	}
}

func TestApiVersionsResponseV3(t *testing.T) {
	const v = 3
	response := new(ApiVersionsResponse)
	response.Version = v
	testVersionDecodable(t, "no error V3", response, apiVersionResponseV3, v)
	assert.Equal(t, int16(ErrNoError), response.ErrorCode)
	assert.Equal(t, []ApiVersionsResponseKey{
		{v, 0, 0, 8},  // API Version Produce (v0-8)
		{v, 1, 0, 11}, // API Version Fetch (v0-11)
		{v, 2, 0, 5},  // API Version Offsets (v0-5)
		{v, 3, 0, 9},  // API Version Metadata (v0-9)
		{v, 4, 0, 4},  // API Version LeaderAndIsr (v0-4)
		{v, 5, 0, 2},  // API Version StopReplica (v0-2)
	}, response.ApiKeys)
	assert.Equal(t, int32(128), response.ThrottleTimeMs)
}

func TestApiVersionsResponseUnsupportedVersion(t *testing.T) {
	t.Run("V0", func(t *testing.T) {
		response := new(ApiVersionsResponse)
		response.Version = 3
		testVersionDecodable(t, "unsupported", response, apiVersionsResponseUnsupportedVersionV0, 3)
		assert.Equal(t, int16(ErrUnsupportedVersion), response.ErrorCode)
		assert.Empty(t, response.ApiKeys)
	})

	t.Run("V1V2", func(t *testing.T) {
		response := new(ApiVersionsResponse)
		response.Version = 3
		testVersionDecodable(t, "unsupported", response, apiVersionsResponseUnsupportedVersionV1V2, 3)
		assert.Equal(t, int16(ErrUnsupportedVersion), response.ErrorCode)
		assert.Empty(t, response.ApiKeys)
	})

	t.Run("V3", func(t *testing.T) {
		response := new(ApiVersionsResponse)
		response.Version = 3
		testVersionDecodable(t, "unsupported", response, apiVersionsResponseUnsupportedVersionV3, 3)
		assert.Equal(t, int16(ErrUnsupportedVersion), response.ErrorCode)
		assert.Equal(t, []ApiVersionsResponseKey{
			{0, 18, 0, 3}, // API Version ApiVersions (v0-3)
		}, response.ApiKeys)
	})

	t.Run("V4", func(t *testing.T) {
		response := new(ApiVersionsResponse)
		response.Version = 4
		testVersionDecodable(t, "unsupported", response, apiVersionsResponseUnsupportedVersionV4, 4)
		assert.Equal(t, int16(ErrUnsupportedVersion), response.ErrorCode)
		assert.Equal(t, []ApiVersionsResponseKey{
			{0, 18, 0, 4}, // API Version ApiVersions (v0-4)
		}, response.ApiKeys)
	})
}

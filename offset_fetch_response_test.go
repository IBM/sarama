//go:build !functional

package sarama

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// version 0 response with one topic "t", one partition 0, offset 0,
	// null metadata (length -1), and error code 7 (ErrRequestTimedOut)
	offsetFetchResponseV0NullMetadata = []byte{
		0x00, 0x00, 0x00, 0x01, // num topics = 1
		0x00, 0x01, 't', // topic name = "t"
		0x00, 0x00, 0x00, 0x01, // num partitions = 1
		0x00, 0x00, 0x00, 0x00, // partition = 0
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset = 0
		0xFF, 0xFF, // metadata = null (length -1)
		0x00, 0x07, // error code = 7
	}

	emptyOffsetFetchResponse = []byte{
		0x00, 0x00, 0x00, 0x00,
	}

	emptyOffsetFetchResponseV2 = []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x2A,
	}

	emptyOffsetFetchResponseV3 = []byte{
		0x00, 0x00, 0x00, 0x09,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x2A,
	}
)

func TestEmptyOffsetFetchResponse(t *testing.T) {
	for version := 0; version <= 1; version++ {
		response := OffsetFetchResponse{Version: int16(version)}
		testResponse(t, fmt.Sprintf("empty v%d", version), &response, emptyOffsetFetchResponse)
	}

	responseV2 := OffsetFetchResponse{Version: 2, Err: ErrInvalidRequest}
	testResponse(t, "empty V2", &responseV2, emptyOffsetFetchResponseV2)

	for version := 3; version <= 5; version++ {
		responseV3 := OffsetFetchResponse{Version: int16(version), Err: ErrInvalidRequest, ThrottleTimeMs: 9}
		testResponse(t, fmt.Sprintf("empty v%d", version), &responseV3, emptyOffsetFetchResponseV3)
	}
}

func TestOffsetFetchResponseNullMetadata(t *testing.T) {
	response := &OffsetFetchResponse{}
	err := versionedDecode(offsetFetchResponseV0NullMetadata, response, 0, nil)
	require.NoError(t, err)

	block := response.GetBlock("t", 0)
	require.NotNil(t, block)
	assert.Empty(t, block.Metadata)
	assert.Equal(t, ErrRequestTimedOut, block.Err)
}

func TestNormalOffsetFetchResponse(t *testing.T) {
	// The response encoded form cannot be checked for it varies due to
	// unpredictable map traversal order.
	// Hence the 'nil' as byte[] parameter in the 'testResponse(..)' calls

	for version := 0; version <= 1; version++ {
		response := OffsetFetchResponse{Version: int16(version)}
		response.AddBlock("t", 0, &OffsetFetchResponseBlock{0, -1, "md", ErrRequestTimedOut})
		response.Blocks["m"] = nil
		testResponse(t, fmt.Sprintf("Normal v%d", version), &response, nil)
	}

	responseV2 := OffsetFetchResponse{Version: 2, Err: ErrInvalidRequest}
	responseV2.AddBlock("t", 0, &OffsetFetchResponseBlock{0, -1, "md", ErrRequestTimedOut})
	responseV2.Blocks["m"] = nil
	testResponse(t, "normal V2", &responseV2, nil)

	for version := 3; version <= 4; version++ {
		responseV3 := OffsetFetchResponse{Version: int16(version), Err: ErrInvalidRequest, ThrottleTimeMs: 9}
		responseV3.AddBlock("t", 0, &OffsetFetchResponseBlock{0, -1, "md", ErrRequestTimedOut})
		responseV3.Blocks["m"] = nil
		testResponse(t, fmt.Sprintf("Normal v%d", version), &responseV3, nil)
	}

	responseV5 := OffsetFetchResponse{Version: 5, Err: ErrInvalidRequest, ThrottleTimeMs: 9}
	responseV5.AddBlock("t", 0, &OffsetFetchResponseBlock{Offset: 10, LeaderEpoch: 100, Metadata: "md", Err: ErrRequestTimedOut})
	responseV5.Blocks["m"] = nil
	testResponse(t, "normal V5", &responseV5, nil)
}

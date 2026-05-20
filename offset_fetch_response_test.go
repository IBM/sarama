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

	emptyOffsetFetchResponseV8 = []byte{
		0x00, 0x00, 0x00, 0x09, // ThrottleTime = 9
		0x02,                     // Groups array, length 1
		0x05, 'b', 'l', 'a', 'h', // GroupId "blah"
		0x01,       // Topics array, length 0
		0x00, 0x2A, // group-level ErrorCode = ErrInvalidRequest
		0x00, // per-group tagged fields
		0x00, // top-level tagged fields
	}

	offsetFetchResponseTwoGroupsV8 = []byte{
		0x00, 0x00, 0x00, 0x09, // ThrottleTime = 9
		0x03,                     // Groups array, length 2
		0x05, 'b', 'l', 'a', 'h', // GroupId "blah"
		0x01,       // Topics array, length 0
		0x00, 0x00, // group-level ErrorCode = ErrNoError
		0x00,                // per-group tagged fields
		0x04, 'q', 'u', 'x', // GroupId "qux"
		0x01,       // Topics array, length 0
		0x00, 0x2A, // group-level ErrorCode = ErrInvalidRequest
		0x00, // per-group tagged fields
		0x00, // top-level tagged fields
	}

	offsetFetchResponseOnePartitionV8 = []byte{
		0x00, 0x00, 0x00, 0x09, // ThrottleTime = 9
		0x02,                     // Groups array, length 1
		0x05, 'b', 'l', 'a', 'h', // GroupId "blah"
		0x02,      // Topics array, length 1
		0x02, 't', // topic name "t"
		0x02,                   // Partitions array, length 1
		0x00, 0x00, 0x00, 0x00, // PartitionIndex = 0
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, // CommittedOffset = 10
		0x00, 0x00, 0x00, 0x64, // CommittedLeaderEpoch = 100
		0x03, 'm', 'd', // Metadata "md"
		0x00, 0x07, // ErrorCode = ErrRequestTimedOut
		0x00,       // per-partition tagged fields
		0x00,       // per-topic tagged fields
		0x00, 0x2A, // group-level ErrorCode = ErrInvalidRequest
		0x00, // per-group tagged fields
		0x00, // top-level tagged fields
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

	responseV8 := OffsetFetchResponse{
		Version:        8,
		ThrottleTimeMs: 9,
		Blocks:         map[string]map[int32]*OffsetFetchResponseBlock{},
		Err:            ErrInvalidRequest,
		Groups: []OffsetFetchResponseGroup{
			{GroupId: "blah", Blocks: map[string]map[int32]*OffsetFetchResponseBlock{}, Err: ErrInvalidRequest},
		},
	}
	testResponse(t, "empty v8", &responseV8, emptyOffsetFetchResponseV8)
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

	responseV8 := OffsetFetchResponse{
		Version:        8,
		ThrottleTimeMs: 9,
		Groups:         []OffsetFetchResponseGroup{{GroupId: "blah", Err: ErrInvalidRequest}},
	}
	responseV8.AddBlock("t", 0, &OffsetFetchResponseBlock{Offset: 10, LeaderEpoch: 100, Metadata: "md", Err: ErrRequestTimedOut})
	responseV8.Blocks = responseV8.Groups[0].Blocks
	responseV8.Err = responseV8.Groups[0].Err
	testResponse(t, "normal v8", &responseV8, offsetFetchResponseOnePartitionV8)
	// GetBlock should route through Groups[0] for v8+
	block := responseV8.GetBlock("t", 0)
	require.NotNil(t, block)
	assert.Equal(t, int64(10), block.Offset)
}

func TestOffsetFetchResponseAddGroupBlockV8(t *testing.T) {
	response := OffsetFetchResponse{Version: 8}
	response.AddGroupBlock("g1", "t1", 0, &OffsetFetchResponseBlock{Offset: 100})
	response.AddGroupBlock("g2", "t1", 0, &OffsetFetchResponseBlock{Offset: 200})

	require.Len(t, response.Groups, 2)
	assert.Equal(t, "g1", response.Groups[0].GroupId)
	assert.Equal(t, int64(100), response.GetGroupBlock("g1", "t1", 0).Offset)
	assert.Equal(t, "g2", response.Groups[1].GroupId)
	assert.Equal(t, int64(200), response.GetGroupBlock("g2", "t1", 0).Offset)

	// Check fallback for v0-7
	responseV7 := OffsetFetchResponse{Version: 7}
	responseV7.AddGroupBlock("g1", "t1", 0, &OffsetFetchResponseBlock{Offset: 300})
	assert.Equal(t, int64(300), responseV7.GetBlock("t1", 0).Offset)
	assert.Nil(t, responseV7.GetGroupBlock("g1", "t1", 0))
}

func TestOffsetFetchResponseMultipleGroupsV8(t *testing.T) {
	response := OffsetFetchResponse{
		Version:        8,
		ThrottleTimeMs: 9,
		Blocks:         map[string]map[int32]*OffsetFetchResponseBlock{},
		Groups: []OffsetFetchResponseGroup{
			{GroupId: "blah", Blocks: map[string]map[int32]*OffsetFetchResponseBlock{}, Err: ErrNoError},
			{GroupId: "qux", Blocks: map[string]map[int32]*OffsetFetchResponseBlock{}, Err: ErrInvalidRequest},
		},
	}
	testResponse(t, "two groups v8", &response, offsetFetchResponseTwoGroupsV8)

	decoded := new(OffsetFetchResponse)
	require.NoError(t, versionedDecode(offsetFetchResponseTwoGroupsV8, decoded, 8, nil))
	require.Len(t, decoded.Groups, 2)
	assert.Equal(t, "blah", decoded.Groups[0].GroupId)
	assert.Equal(t, ErrNoError, decoded.Groups[0].Err)
	assert.Equal(t, "qux", decoded.Groups[1].GroupId)
	assert.Equal(t, ErrInvalidRequest, decoded.Groups[1].Err)
	assert.Equal(t, ErrNoError, decoded.GroupError())
}

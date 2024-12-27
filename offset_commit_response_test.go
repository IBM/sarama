//go:build !functional

package sarama

import (
	"fmt"
	"reflect"
	"testing"
)

var (
	emptyOffsetCommitResponseV0 = []byte{
		0x00, 0x00, 0x00, 0x00, // Empty topic
	}
	noEmptyOffsetCommitResponseV0 = []byte{
		0, 0, 0, 1, // Topic Len
		0, 5, 't', 'o', 'p', 'i', 'c', // Name
		0, 0, 0, 1, // Partition Len
		0, 0, 0, 3, // PartitionIndex
		0, 0, // ErrorCode
	}
	noEmptyOffsetCommitResponseV3 = []byte{
		0, 0, 0, 100, // ThrottleTimeMs
		0, 0, 0, 1, // Topic Len
		0, 5, 't', 'o', 'p', 'i', 'c', // Name
		0, 0, 0, 1, // Partition Len
		0, 0, 0, 3, // PartitionIndex
		0, 0, // ErrorCode
	}
)

func TestEmptyOffsetCommitResponse(t *testing.T) {
	// groupInstanceId := "gid"
	tests := []struct {
		CaseName     string
		Version      int16
		MessageBytes []byte
		Message      *OffsetCommitResponse
	}{
		{
			"v0-empty",
			0,
			emptyOffsetCommitResponseV0,
			&OffsetCommitResponse{
				Version: 0,
			},
		},
		{
			"v0-two-partition",
			0,
			noEmptyOffsetCommitResponseV0,
			&OffsetCommitResponse{
				Version: 0,
				Errors: map[string]map[int32]KError{
					"topic": {
						3: ErrNoError,
					},
				},
			},
		},
		{
			"v3",
			3,
			noEmptyOffsetCommitResponseV3,
			&OffsetCommitResponse{
				ThrottleTimeMs: 100,
				Version:        3,
				Errors: map[string]map[int32]KError{
					"topic": {
						3: ErrNoError,
					},
				},
			},
		},
	}
	for _, c := range tests {
		response := new(OffsetCommitResponse)
		testVersionDecodable(t, c.CaseName, response, c.MessageBytes, c.Version)
		if !reflect.DeepEqual(c.Message, response) {
			t.Errorf("case %s decode failed, expected:%+v got %+v", c.CaseName, c.Message, response)
		}
		testEncodable(t, c.CaseName, c.Message, c.MessageBytes)
	}
}

func TestNormalOffsetCommitResponse(t *testing.T) {
	response := OffsetCommitResponse{}
	response.AddError("t", 0, ErrNotLeaderForPartition)
	response.Errors["m"] = make(map[int32]KError)
	// The response encoded form cannot be checked for it varies due to
	// unpredictable map traversal order.
	testResponse(t, "normal", &response, nil)
}

func TestOffsetCommitResponseWithThrottleTime(t *testing.T) {
	for version := 3; version <= 4; version++ {
		response := OffsetCommitResponse{
			Version:        int16(version),
			ThrottleTimeMs: 123,
		}
		response.AddError("t", 0, ErrNotLeaderForPartition)
		response.Errors["m"] = make(map[int32]KError)
		// The response encoded form cannot be checked for it varies due to
		// unpredictable map traversal order.
		testResponse(t, fmt.Sprintf("v%d with throttle time", version), &response, nil)
	}
}

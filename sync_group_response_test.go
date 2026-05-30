//go:build !functional

package sarama

import (
	"reflect"
	"testing"
)

var (
	syncGroupResponseV0NoError = []byte{
		0x00, 0x00, // No error
		0, 0, 0, 3, 0x01, 0x02, 0x03, // Member assignment data
	}

	syncGroupResponseV0WithError = []byte{
		0, 27, // ErrRebalanceInProgress
		0, 0, 0, 0, // No member assignment data
	}

	syncGroupResponseV1NoError = []byte{
		0, 0, 0, 100, // ThrottleTimeMs
		0x00, 0x00, // No error
		0, 0, 0, 3, 0x01, 0x02, 0x03, // Member assignment data
	}

	syncGroupResponseV4NoError = []byte{
		0, 0, 0, 100, // ThrottleTimeMs
		0x00, 0x00, // No error
		4, 0x01, 0x02, 0x03, // Member assignment data
		0, // empty tagged fields
	}

	syncGroupResponseV5NoError = []byte{
		0, 0, 0, 100, // ThrottleTimeMs
		0x00, 0x00, // No error
		9, 'c', 'o', 'n', 's', 'u', 'm', 'e', 'r', // ProtocolType
		6, 'r', 'a', 'n', 'g', 'e', // ProtocolName
		4, 0x01, 0x02, 0x03, // Member assignment data
		0, // empty tagged fields
	}
)

func TestSyncGroupResponse(t *testing.T) {
	protocolType := "consumer"
	protocolName := "range"
	tests := []struct {
		CaseName     string
		Version      int16
		MessageBytes []byte
		Message      *SyncGroupResponse
	}{
		{
			"v0-noErr",
			0,
			syncGroupResponseV0NoError,
			&SyncGroupResponse{
				Version:          0,
				Err:              ErrNoError,
				MemberAssignment: []byte{1, 2, 3},
			},
		},
		{
			"v0-Err",
			0,
			syncGroupResponseV0WithError,
			&SyncGroupResponse{
				Version:          0,
				Err:              ErrRebalanceInProgress,
				MemberAssignment: []byte{},
			},
		},
		{
			"v1-noErr",
			1,
			syncGroupResponseV1NoError,
			&SyncGroupResponse{
				ThrottleTime:     100,
				Version:          1,
				Err:              ErrNoError,
				MemberAssignment: []byte{1, 2, 3},
			},
		},
		{
			"v4-noErr",
			4,
			syncGroupResponseV4NoError,
			&SyncGroupResponse{
				ThrottleTime:     100,
				Version:          4,
				Err:              ErrNoError,
				MemberAssignment: []byte{1, 2, 3},
			},
		},
		{
			"v5-noErr",
			5,
			syncGroupResponseV5NoError,
			&SyncGroupResponse{
				ThrottleTime:     100,
				Version:          5,
				Err:              ErrNoError,
				ProtocolType:     &protocolType,
				ProtocolName:     &protocolName,
				MemberAssignment: []byte{1, 2, 3},
			},
		},
	}
	for _, c := range tests {
		response := new(SyncGroupResponse)
		testVersionDecodable(t, c.CaseName, response, c.MessageBytes, c.Version)
		if !reflect.DeepEqual(c.Message, response) {
			t.Errorf("case %s decode failed, expected:%+v got %+v", c.CaseName, c.Message, response)
		}
		testEncodable(t, c.CaseName, c.Message, c.MessageBytes)
	}
}

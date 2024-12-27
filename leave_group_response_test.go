//go:build !functional

package sarama

import (
	"reflect"
	"testing"
)

var (
	leaveGroupResponseV0NoError   = []byte{0x00, 0x00}
	leaveGroupResponseV0WithError = []byte{0, 25}
	leaveGroupResponseV1NoError   = []byte{
		0, 0, 0, 100, // ThrottleTime
		0x00, 0x00, // Err
	}
	leaveGroupResponseV3NoError = []byte{
		0, 0, 0, 100, // ThrottleTime
		0x00, 0x00, // Err
		0, 0, 0, 2, // Two Members
		0, 4, 'm', 'i', 'd', '1', // MemberId
		255, 255, // GroupInstanceId
		0, 0, // Err
		0, 4, 'm', 'i', 'd', '2', // MemberId
		0, 3, 'g', 'i', 'd', // GroupInstanceId
		0, 25, // Err
	}
)

func TestLeaveGroupResponse(t *testing.T) {
	groupInstanceId := "gid"
	tests := []struct {
		CaseName     string
		Version      int16
		MessageBytes []byte
		Message      *LeaveGroupResponse
	}{
		{
			"v0-noErr",
			0,
			leaveGroupResponseV0NoError,
			&LeaveGroupResponse{
				Version: 0,
				Err:     ErrNoError,
			},
		},
		{
			"v0-Err",
			0,
			leaveGroupResponseV0WithError,
			&LeaveGroupResponse{
				Version: 0,
				Err:     ErrUnknownMemberId,
			},
		},
		{
			"v1-noErr",
			1,
			leaveGroupResponseV1NoError,
			&LeaveGroupResponse{
				Version:      1,
				ThrottleTime: 100,
				Err:          ErrNoError,
			},
		},
		{
			"v3",
			3,
			leaveGroupResponseV3NoError,
			&LeaveGroupResponse{
				Version:      3,
				ThrottleTime: 100,
				Err:          ErrNoError,
				Members: []MemberResponse{
					{"mid1", nil, ErrNoError},
					{"mid2", &groupInstanceId, ErrUnknownMemberId},
				},
			},
		},
	}
	for _, c := range tests {
		response := new(LeaveGroupResponse)
		testVersionDecodable(t, c.CaseName, response, c.MessageBytes, c.Version)
		if !reflect.DeepEqual(c.Message, response) {
			t.Errorf("case %s decode failed, expected:%+v got %+v", c.CaseName, c.Message, response)
		}
		testEncodable(t, c.CaseName, c.Message, c.MessageBytes)
	}
}

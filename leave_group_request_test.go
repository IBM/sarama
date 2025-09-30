//go:build !functional

package sarama

import (
	"reflect"
	"testing"
)

var (
	basicLeaveGroupRequestV0 = []byte{
		0, 3, 'f', 'o', 'o',
		0, 3, 'b', 'a', 'r',
	}
	basicLeaveGroupRequestV3 = []byte{
		0, 3, 'f', 'o', 'o',
		0, 0, 0, 2, // Two Member
		0, 4, 'm', 'i', 'd', '1', // MemberId
		255, 255, // GroupInstanceId  nil
		0, 4, 'm', 'i', 'd', '2', // MemberId
		0, 3, 'g', 'i', 'd', // GroupInstanceId
	}
	basicLeaveGroupRequestV4 = []byte{
		4, 'f', 'o', 'o',
		3,                     // Two Member
		5, 'm', 'i', 'd', '1', // MemberId
		0,                     // GroupInstanceId  nil
		0,                     // empty tagged fields
		5, 'm', 'i', 'd', '2', // MemberId
		4, 'g', 'i', 'd', // GroupInstanceId
		0, // empty tagged fields
		0, // empty tagged fields
	}
)

func TestLeaveGroupRequest(t *testing.T) {
	groupInstanceId := "gid"
	tests := []struct {
		CaseName     string
		Version      int16
		MessageBytes []byte
		Message      *LeaveGroupRequest
	}{
		{
			"v0",
			0,
			basicLeaveGroupRequestV0,
			&LeaveGroupRequest{
				Version:  0,
				GroupId:  "foo",
				MemberId: "bar",
			},
		},
		{
			"v3",
			3,
			basicLeaveGroupRequestV3,
			&LeaveGroupRequest{
				Version: 3,
				GroupId: "foo",
				Members: []MemberIdentity{
					{"mid1", nil},
					{"mid2", &groupInstanceId},
				},
			},
		},
		{
			"v4",
			4,
			basicLeaveGroupRequestV4,
			&LeaveGroupRequest{
				Version: 4,
				GroupId: "foo",
				Members: []MemberIdentity{
					{"mid1", nil},
					{"mid2", &groupInstanceId},
				},
			},
		},
	}
	for _, c := range tests {
		request := new(LeaveGroupRequest)
		testVersionDecodable(t, c.CaseName, request, c.MessageBytes, c.Version)
		if !reflect.DeepEqual(c.Message, request) {
			t.Errorf("case %s decode failed, expected:%+v got %+v", c.CaseName, c.Message, request)
		}
		testEncodable(t, c.CaseName, c.Message, c.MessageBytes)
	}
}

//go:build !functional

package sarama

import (
	"reflect"
	"testing"
)

var (
	emptySyncGroupRequest = []byte{
		0, 3, 'f', 'o', 'o', // Group ID
		0x00, 0x01, 0x02, 0x03, // Generation ID
		0, 3, 'b', 'a', 'z', // Member ID
		0, 0, 0, 0, // no assignments
	}

	populatedSyncGroupRequest = []byte{
		0, 3, 'f', 'o', 'o', // Group ID
		0x00, 0x01, 0x02, 0x03, // Generation ID
		0, 3, 'b', 'a', 'z', // Member ID
		0, 0, 0, 1, // one assignment
		0, 3, 'b', 'a', 'z', // Member ID
		0, 0, 0, 3, 'f', 'o', 'o', // Member assignment
	}
)

func TestSyncGroupRequest(t *testing.T) {
	var request *SyncGroupRequest

	request = new(SyncGroupRequest)
	request.GroupId = "foo"
	request.GenerationId = 66051
	request.MemberId = "baz"
	testRequest(t, "empty", request, emptySyncGroupRequest)

	request = new(SyncGroupRequest)
	request.GroupId = "foo"
	request.GenerationId = 66051
	request.MemberId = "baz"
	request.AddGroupAssignment("baz", []byte("foo"))
	testRequest(t, "populated", request, populatedSyncGroupRequest)
}

var (
	populatedSyncGroupRequestV3 = []byte{
		0, 3, 'f', 'o', 'o', // Group ID
		0x00, 0x01, 0x02, 0x03, // Generation ID
		0, 3, 'b', 'a', 'z', // Member ID
		0, 3, 'g', 'i', 'd', // GroupInstance ID
		0, 0, 0, 1, // one assignment
		0, 3, 'b', 'a', 'z', // Member ID
		0, 0, 0, 3, 'f', 'o', 'o', // Member assignment
	}
)

func TestSyncGroupRequestV3AndPlus(t *testing.T) {
	groupInstanceId := "gid"
	tests := []struct {
		CaseName     string
		Version      int16
		MessageBytes []byte
		Message      *SyncGroupRequest
	}{
		{
			"v3",
			3,
			populatedSyncGroupRequestV3,
			&SyncGroupRequest{
				Version:         3,
				GroupId:         "foo",
				GenerationId:    0x00010203,
				MemberId:        "baz",
				GroupInstanceId: &groupInstanceId,
				GroupAssignments: []SyncGroupRequestAssignment{
					{
						MemberId:   "baz",
						Assignment: []byte("foo"),
					},
				},
			},
		},
	}
	for _, c := range tests {
		request := new(SyncGroupRequest)
		testVersionDecodable(t, c.CaseName, request, c.MessageBytes, c.Version)
		if !reflect.DeepEqual(c.Message, request) {
			t.Errorf("case %s decode failed, expected:%+v got %+v", c.CaseName, c.Message, request)
		}
		testEncodable(t, c.CaseName, c.Message, c.MessageBytes)
	}
}

//go:build !functional

package sarama

import (
	"reflect"
	"testing"
)

var (
	basicHeartbeatRequestV0 = []byte{
		0, 3, 'f', 'o', 'o', // Group ID
		0x00, 0x01, 0x02, 0x03, // Generation ID
		0, 3, 'b', 'a', 'z', // Member ID
	}

	basicHeartbeatRequestV3_GID = []byte{
		0, 3, 'f', 'o', 'o', // Group ID
		0x00, 0x01, 0x02, 0x03, // Generation ID
		0, 3, 'b', 'a', 'z', // Member ID
		0, 3, 'g', 'i', 'd', // Group Instance ID
	}
	basicHeartbeatRequestV3_NOGID = []byte{
		0, 3, 'f', 'o', 'o', // Group ID
		0x00, 0x01, 0x02, 0x03, // Generation ID
		0, 3, 'b', 'a', 'z', // Member ID
		255, 255, // Group Instance ID
	}
)

func TestHeartbeatRequest(t *testing.T) {
	groupInstanceId := "gid"
	tests := []struct {
		CaseName     string
		Version      int16
		MessageBytes []byte
		Message      *HeartbeatRequest
	}{
		{
			"v0_basic",
			0,
			basicHeartbeatRequestV0,
			&HeartbeatRequest{
				Version:      0,
				GroupId:      "foo",
				GenerationId: 0x00010203,
				MemberId:     "baz",
			},
		},
		{
			"v3_basic",
			3,
			basicHeartbeatRequestV3_GID,
			&HeartbeatRequest{
				Version:         3,
				GroupId:         "foo",
				GenerationId:    0x00010203,
				MemberId:        "baz",
				GroupInstanceId: &groupInstanceId,
			},
		},
		{
			"v3_basic",
			3,
			basicHeartbeatRequestV3_NOGID,
			&HeartbeatRequest{
				Version:         3,
				GroupId:         "foo",
				GenerationId:    0x00010203,
				MemberId:        "baz",
				GroupInstanceId: nil,
			},
		},
	}
	for _, c := range tests {
		testEncodable(t, c.CaseName, c.Message, c.MessageBytes)
		request := new(HeartbeatRequest)
		testVersionDecodable(t, c.CaseName, request, c.MessageBytes, c.Version)
		if !reflect.DeepEqual(c.Message, request) {
			t.Errorf("case %s decode failed, expected:%+v got %+v", c.CaseName, c.Message, request)
		}
	}
}

//go:build !functional

package sarama

import (
	"reflect"
	"testing"
)

var (
	heartbeatResponseNoError_V0 = []byte{
		0x00, 0x00,
	}
	heartbeatResponseNoError_V1 = []byte{
		0, 0, 0, 100,
		0, 0,
	}
	heartbeatResponseError_V1 = []byte{
		0, 0, 0, 100,
		0, byte(ErrFencedInstancedId),
	}
)

func TestHeartbeatResponse(t *testing.T) {
	tests := []struct {
		CaseName     string
		Version      int16
		MessageBytes []byte
		Message      *HeartbeatResponse
	}{
		{
			"v0_noErr",
			0,
			heartbeatResponseNoError_V0,
			&HeartbeatResponse{
				Version: 0,
				Err:     ErrNoError,
			},
		},
		{
			"v1_noErr",
			1,
			heartbeatResponseNoError_V1,
			&HeartbeatResponse{
				Version:      1,
				Err:          ErrNoError,
				ThrottleTime: 100,
			},
		},
		{
			"v1_Err",
			1,
			heartbeatResponseError_V1,
			&HeartbeatResponse{
				Version:      1,
				Err:          ErrFencedInstancedId,
				ThrottleTime: 100,
			},
		},
	}
	for _, c := range tests {
		testEncodable(t, c.CaseName, c.Message, c.MessageBytes)
		response := new(HeartbeatResponse)
		testVersionDecodable(t, c.CaseName, response, c.MessageBytes, c.Version)
		if !reflect.DeepEqual(c.Message, response) {
			t.Errorf("case %s decode failed, expected:%+v got %+v", c.CaseName, c.Message, response)
		}
	}
}

//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var (
	endTxnResponse = []byte{
		0, 0, 0, 100,
		0, 49,
	}

	endTxnResponseV3 = []byte{
		0, 0, 0, 100, // ThrottleTimeMs
		0, 49, // ErrorCode
		0, // tagged fields
	}
)

func TestEndTxnResponse(t *testing.T) {
	resp := &EndTxnResponse{
		ThrottleTime: 100 * time.Millisecond,
		Err:          ErrInvalidProducerIDMapping,
	}

	testResponse(t, "v0", resp, endTxnResponse)

	resp.Version = 3
	testResponse(t, "v3", resp, endTxnResponseV3)
}

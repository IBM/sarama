//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var (
	addOffsetsToTxnResponse = []byte{
		0, 0, 0, 100,
		0, 47,
	}

	addOffsetsToTxnResponseV3 = []byte{
		0, 0, 0, 100, // ThrottleTimeMs
		0, 47, // ErrorCode
		0, // tagged fields
	}
)

func TestAddOffsetsToTxnResponse(t *testing.T) {
	resp := &AddOffsetsToTxnResponse{
		ThrottleTime: 100 * time.Millisecond,
		Err:          ErrInvalidProducerEpoch,
	}

	testResponse(t, "v0", resp, addOffsetsToTxnResponse)

	resp.Version = 3
	testResponse(t, "v3", resp, addOffsetsToTxnResponseV3)
}

//go:build !functional

package sarama

import (
	"testing"
	"time"

	assert "github.com/stretchr/testify/require"
)

var (
	endTxnResponse = []byte{
		0, 0, 0, 100,
		0, 49,
	}

	endTxnResponseV3 = []byte{
		0, 0, 0, 100,
		0, 0,
		0x00, // empty tagged fields
	}

	endTxnResponseV5 = []byte{
		0, 0, 0, 100,
		0, 0,
		0, 0, 0, 0, 0, 0, 0, 42, // ProducerID
		0, 7, // ProducerEpoch
		0x00, // empty tagged fields
	}
)

func TestEndTxnResponse(t *testing.T) {
	resp := &EndTxnResponse{
		ThrottleTime:  100 * time.Millisecond,
		Err:           ErrInvalidProducerIDMapping,
		ProducerID:    -1,
		ProducerEpoch: -1,
	}

	testResponse(t, "", resp, endTxnResponse)
}

func TestEndTxnResponseV3(t *testing.T) {
	resp := &EndTxnResponse{
		Version:       3,
		ThrottleTime:  100 * time.Millisecond,
		Err:           ErrNoError,
		ProducerID:    -1,
		ProducerEpoch: -1,
	}

	testResponse(t, "v3", resp, endTxnResponseV3)
}

func TestEndTxnResponseV5(t *testing.T) {
	resp := &EndTxnResponse{
		Version:       5,
		ThrottleTime:  100 * time.Millisecond,
		Err:           ErrNoError,
		ProducerID:    42,
		ProducerEpoch: 7,
	}

	testResponse(t, "v5", resp, endTxnResponseV5)
}

func TestEndTxnResponseV3DefaultsToUnsetEpoch(t *testing.T) {
	decoded := new(EndTxnResponse)
	assert.NoError(t, versionedDecode(endTxnResponseV3, decoded, 3, nil))
	assert.Equal(t, int64(-1), decoded.ProducerID)
	assert.Equal(t, int16(-1), decoded.ProducerEpoch)
}

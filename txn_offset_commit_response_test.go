//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var (
	txnOffsetCommitResponse = []byte{
		0, 0, 0, 100,
		0, 0, 0, 1, // 1 topic
		0, 5, 't', 'o', 'p', 'i', 'c',
		0, 0, 0, 1, // 1 partition response
		0, 0, 0, 2, // partition number 2
		0, 47, // err
	}

	txnOffsetCommitResponseV3 = []byte{
		0, 0, 0, 100, // throttle time
		2,                          // 1 topic (compact array)
		6, 't', 'o', 'p', 'i', 'c', // topic name (compact string)
		2,          // 1 partition response (compact array)
		0, 0, 0, 2, // partition number 2
		0, 47, // err
		0, // partition tagged fields
		0, // topic tagged fields
		0, // response tagged fields
	}
)

func TestTxnOffsetCommitResponse(t *testing.T) {
	resp := &TxnOffsetCommitResponse{
		ThrottleTime: 100 * time.Millisecond,
		Topics: map[string][]*PartitionError{
			"topic": {{
				Partition: 2,
				Err:       ErrInvalidProducerEpoch,
			}},
		},
	}

	testResponse(t, "", resp, txnOffsetCommitResponse)
}

func TestTxnOffsetCommitResponseV3(t *testing.T) {
	resp := &TxnOffsetCommitResponse{
		Version:      3,
		ThrottleTime: 100 * time.Millisecond,
		Topics: map[string][]*PartitionError{
			"topic": {{
				Partition: 2,
				Err:       ErrInvalidProducerEpoch,
			}},
		},
	}

	testResponse(t, "V3", resp, txnOffsetCommitResponseV3)
}

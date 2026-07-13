//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var describeTransactionsResponseV0 = []byte{
	0, 0, 0, 100, // throttleTimeMs
	2,    // TransactionStates (one element array)
	0, 0, // error code
	4, 't', 'x', 'n', // transactional id
	8, 'O', 'n', 'g', 'o', 'i', 'n', 'g', // transaction state
	0, 0, 234, 96, // transaction timeout ms
	0, 0, 0, 0, 0, 0, 0, 8, // transaction start time ms
	0, 0, 0, 0, 0, 0, 3, 232, // producer id
	0, 1, // producer epoch
	2,                          // Topics (one element array)
	6, 't', 'o', 'p', 'i', 'c', // topic
	2,          // Partitions (one element array)
	0, 0, 0, 0, // partition 0
	0, // empty tagged fields
	0, // empty tagged fields
	0, // empty tagged fields
}

func TestDescribeTransactionsResponse(t *testing.T) {
	response := &DescribeTransactionsResponse{
		Version:      0,
		ThrottleTime: 100 * time.Millisecond,
		TransactionStates: []TransactionState{
			{
				ErrorCode:            ErrNoError,
				TransactionalID:      "txn",
				TransactionState:     "Ongoing",
				TransactionTimeout:   time.Minute,
				TransactionStartTime: 8,
				ProducerID:           1000,
				ProducerEpoch:        1,
				Topics: []DescribeTransactionsResponseTopic{
					{
						Topic:      "topic",
						Partitions: []int32{0},
					},
				},
			},
		},
	}

	testResponse(t, "v0", response, describeTransactionsResponseV0)
}

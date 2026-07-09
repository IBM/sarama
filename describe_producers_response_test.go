//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var describeProducersResponseV0 = []byte{
	0, 0, 0, 100, // throttleTimeMs
	2,                          // Topics
	6, 't', 'o', 'p', 'i', 'c', // name
	2,          // Partitions
	0, 0, 0, 0, // partition index
	0, 0, // error code
	0,                        // error message
	2,                        // ActiveProducers
	0, 0, 0, 0, 0, 0, 3, 232, // producer id
	0, 0, 0, 1, // producer epoch
	0, 0, 0, 5, // last sequence
	0, 0, 0, 0, 0, 0, 0, 8, // last timestamp
	0, 0, 0, 2, // coordinator epoch
	255, 255, 255, 255, 255, 255, 255, 255, // current txn start offset
	0, // empty tagged fields
	0, // empty tagged fields
	0, // empty tagged fields
	0, // empty tagged fields
}

func TestDescribeProducersResponse(t *testing.T) {
	response := &DescribeProducersResponse{
		Version:      0,
		ThrottleTime: 100 * time.Millisecond,
		Topics: []DescribeProducersResponseTopic{
			{
				Name: "topic",
				Partitions: []DescribeProducersResponsePartition{
					{
						PartitionIndex: 0,
						ErrorCode:      ErrNoError,
						ActiveProducers: []ProducerState{
							{
								ProducerID:            1000,
								ProducerEpoch:         1,
								LastSequence:          5,
								LastTimestamp:         8,
								CoordinatorEpoch:      2,
								CurrentTxnStartOffset: -1,
							},
						},
					},
				},
			},
		},
	}

	testResponse(t, "v0", response, describeProducersResponseV0)
}

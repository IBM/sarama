//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var deleteRecordsResponse = []byte{
	0, 0, 0, 100,
	0, 0, 0, 2,
	0, 5, 'o', 't', 'h', 'e', 'r',
	0, 0, 0, 0,
	0, 5, 't', 'o', 'p', 'i', 'c',
	0, 0, 0, 2,
	0, 0, 0, 19,
	0, 0, 0, 0, 0, 0, 0, 200,
	0, 0,
	0, 0, 0, 20,
	255, 255, 255, 255, 255, 255, 255, 255,
	0, 3,
}

var deleteRecordsResponseV2 = []byte{
	0, 0, 0, 100, // ThrottleTimeMs 100
	0x03,                          // Topics compact array length (2+1)
	0x06, 'o', 't', 'h', 'e', 'r', // "other" compact string (5+1)
	0x01,                          // Partitions compact array length (0+1, empty)
	0x00,                          // topic tagged fields
	0x06, 't', 'o', 'p', 'i', 'c', // "topic" compact string (5+1)
	0x03,        // Partitions compact array length (2+1)
	0, 0, 0, 19, // PartitionIndex 19
	0, 0, 0, 0, 0, 0, 0, 200, // LowWatermark 200
	0, 0, // ErrorCode 0
	0x00,        // partition tagged fields
	0, 0, 0, 20, // PartitionIndex 20
	255, 255, 255, 255, 255, 255, 255, 255, // LowWatermark -1
	0, 3, // ErrorCode 3
	0x00, // partition tagged fields
	0x00, // topic tagged fields
	0x00, // response tagged fields
}

func TestDeleteRecordsResponse(t *testing.T) {
	t.Run("v0", func(t *testing.T) {
		resp := &DeleteRecordsResponse{
			Version:      0,
			ThrottleTime: 100 * time.Millisecond,
			Topics: map[string]*DeleteRecordsResponseTopic{
				"topic": {
					Partitions: map[int32]*DeleteRecordsResponsePartition{
						19: {LowWatermark: 200, Err: 0},
						20: {LowWatermark: -1, Err: 3},
					},
				},
				"other": {},
			},
		}

		testResponse(t, "", resp, deleteRecordsResponse)
	})

	t.Run("v2 flexible", func(t *testing.T) {
		resp := &DeleteRecordsResponse{
			Version:      2,
			ThrottleTime: 100 * time.Millisecond,
			Topics: map[string]*DeleteRecordsResponseTopic{
				"topic": {
					Partitions: map[int32]*DeleteRecordsResponsePartition{
						19: {LowWatermark: 200, Err: 0},
						20: {LowWatermark: -1, Err: 3},
					},
				},
				"other": {},
			},
		}

		testResponse(t, "", resp, deleteRecordsResponseV2)
	})
}

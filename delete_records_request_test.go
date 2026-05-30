//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var deleteRecordsRequest = []byte{
	0, 0, 0, 2,
	0, 5, 'o', 't', 'h', 'e', 'r',
	0, 0, 0, 0,
	0, 5, 't', 'o', 'p', 'i', 'c',
	0, 0, 0, 2,
	0, 0, 0, 19,
	0, 0, 0, 0, 0, 0, 0, 200,
	0, 0, 0, 20,
	0, 0, 0, 0, 0, 0, 0, 190,
	0, 0, 0, 100,
}

var deleteRecordsRequestV2 = []byte{
	0x03,                          // Topics compact array length (2+1)
	0x06, 'o', 't', 'h', 'e', 'r', // "other" compact string (5+1)
	0x01,                          // Partitions compact array length (0+1, empty)
	0x00,                          // topic tagged fields
	0x06, 't', 'o', 'p', 'i', 'c', // "topic" compact string (5+1)
	0x03,        // Partitions compact array length (2+1)
	0, 0, 0, 19, // PartitionIndex 19
	0, 0, 0, 0, 0, 0, 0, 200, // Offset 200
	0x00,        // partition tagged fields
	0, 0, 0, 20, // PartitionIndex 20
	0, 0, 0, 0, 0, 0, 0, 190, // Offset 190
	0x00,         // partition tagged fields
	0x00,         // topic tagged fields
	0, 0, 0, 100, // TimeoutMs 100
	0x00, // request tagged fields
}

func TestDeleteRecordsRequest(t *testing.T) {
	t.Run("v0", func(t *testing.T) {
		req := &DeleteRecordsRequest{
			Topics: map[string]*DeleteRecordsRequestTopic{
				"topic": {
					PartitionOffsets: map[int32]int64{
						19: 200,
						20: 190,
					},
				},
				"other": {},
			},
			Timeout: 100 * time.Millisecond,
		}

		testRequest(t, "", req, deleteRecordsRequest)
	})

	t.Run("v2 flexible", func(t *testing.T) {
		req := &DeleteRecordsRequest{
			Version: 2,
			Topics: map[string]*DeleteRecordsRequestTopic{
				"topic": {
					PartitionOffsets: map[int32]int64{
						19: 200,
						20: 190,
					},
				},
				"other": {},
			},
			Timeout: 100 * time.Millisecond,
		}

		testRequest(t, "", req, deleteRecordsRequestV2)
	})
}

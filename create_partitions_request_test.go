//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var (
	createPartitionRequestNoAssignment = []byte{
		0, 0, 0, 1, // one topic
		0, 5, 't', 'o', 'p', 'i', 'c',
		0, 0, 0, 3, // 3 partitions
		255, 255, 255, 255, // no assignments
		0, 0, 0, 100, // timeout
		0, // validate only = false
	}

	createPartitionRequestAssignment = []byte{
		0, 0, 0, 1,
		0, 5, 't', 'o', 'p', 'i', 'c',
		0, 0, 0, 3, // 3 partitions
		0, 0, 0, 2,
		0, 0, 0, 2,
		0, 0, 0, 2, 0, 0, 0, 3,
		0, 0, 0, 2,
		0, 0, 0, 3, 0, 0, 0, 1,
		0, 0, 0, 100,
		1, // validate only = true
	}

	createPartitionRequestAssignmentV2 = []byte{
		2,
		6, 't', 'o', 'p', 'i', 'c',
		0, 0, 0, 3, // 3 partitions
		3,
		3,
		0, 0, 0, 2, 0, 0, 0, 3,
		0, // empty tagged fields
		3,
		0, 0, 0, 3, 0, 0, 0, 1,
		0, // empty tagged fields
		0, // empty tagged fields
		0, 0, 0, 100,
		1, // validate only = true
		0, // empty tagged fields
	}
)

func TestCreatePartitionsRequest(t *testing.T) {
	req := &CreatePartitionsRequest{
		TopicPartitions: map[string]*TopicPartition{
			"topic": {
				Count: 3,
			},
		},
		Timeout: 100 * time.Millisecond,
	}

	buf := testRequestEncode(t, "no assignment", req, createPartitionRequestNoAssignment)
	testRequestDecode(t, "no assignment", req, buf)

	req.ValidateOnly = true
	req.TopicPartitions["topic"].Assignment = [][]int32{{2, 3}, {3, 1}}

	buf = testRequestEncode(t, "assignment", req, createPartitionRequestAssignment)
	testRequestDecode(t, "assignment", req, buf)

	req.Version = 2
	buf = testRequestEncode(t, "assignment v2", req, createPartitionRequestAssignmentV2)
	testRequestDecode(t, "assignment v2", req, buf)

	// v3 wire format is identical to v2
	req.Version = 3
	buf = testRequestEncode(t, "assignment v3", req, createPartitionRequestAssignmentV2)
	testRequestDecode(t, "assignment v3", req, buf)
}

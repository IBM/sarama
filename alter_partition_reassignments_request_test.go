//go:build !functional

package sarama

import "testing"

var (
	alterPartitionReassignmentsRequestNoBlock = []byte{
		0, 0, 39, 16, // timeout 10000
		1, // 1-1=0 blocks
		0, // empty tagged fields
	}

	alterPartitionReassignmentsRequestOneBlock = []byte{
		0, 0, 39, 16, // timeout 10000
		2,                         // 2-1=1 block
		6, 116, 111, 112, 105, 99, // topic name "topic" as compact string
		2,          // 2-1=1 partitions
		0, 0, 0, 0, // partitionId
		3,            // 3-1=2 replica array size
		0, 0, 3, 232, // replica 1000
		0, 0, 3, 233, // replica 1001
		0, 0, 0, // empty tagged fields
	}

	alterPartitionReassignmentsAbortRequest = []byte{
		0, 0, 39, 16, // timeout 10000
		2,                         // 2-1=1 block
		6, 116, 111, 112, 105, 99, // topic name "topic" as compact string
		2,          // 2-1=1 partitions
		0, 0, 0, 0, // partitionId
		0,       // replica array is null (indicates that a pending reassignment should be aborted)
		0, 0, 0, // empty tagged fields
	}
)

func TestAlterPartitionReassignmentRequest(t *testing.T) {
	var request *AlterPartitionReassignmentsRequest

	request = &AlterPartitionReassignmentsRequest{
		TimeoutMs: int32(10000),
		Version:   int16(0),
	}

	testRequest(t, "no block", request, alterPartitionReassignmentsRequestNoBlock)

	request.AddBlock("topic", 0, []int32{1000, 1001})

	testRequest(t, "one block", request, alterPartitionReassignmentsRequestOneBlock)

	request = &AlterPartitionReassignmentsRequest{
		TimeoutMs: int32(10000),
		Version:   int16(0),
	}
	request.AddBlock("topic", 0, nil)

	testRequest(t, "abort assignment", request, alterPartitionReassignmentsAbortRequest)
}

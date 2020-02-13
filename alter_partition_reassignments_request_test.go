package sarama

import "testing"

var (
	alterPartitionReassignmentsRequestNoBlock = []byte{
		0, 0, 39, 16, // timout 10000
		1, // 1-1=0 blocks
		0, // empty tagged fields
	}

	alterPartitionReassignmentsRequestOneBlock = []byte{
		0, 0, 39, 16, // timout 10000
		2,                         // 2-1=1 block
		6, 116, 111, 112, 105, 99, // topic name "topic" as compact string
		2,          // 2-1=1 partitions
		0, 0, 0, 0, // partitionId
		3,            // 3-1=2 replica array size
		0, 0, 3, 232, // replica 1000
		0, 0, 3, 233, // replica 1001
		0, 0, 0, // empty tagged fields
	}

	alterPartitionReassignmentsRequestTwoBlocks = []byte{
		0, 0, 39, 16, // timout 10000
		3,                         // 3-1=2 blocks
		6, 116, 111, 112, 105, 99, // topic name "topic" as compact string
		2,          // 2-1=1 partitions
		0, 0, 0, 0, // partitionId
		3,            // 3-1=2 replica array size
		0, 0, 3, 232, // replica 1000
		0, 0, 3, 233, // replica 1001
		0, 0, // empty tagged fields
		7, 116, 111, 112, 105, 99, 50, // topic name "topic2" as compact string
		2,          // 2-1=1 partitions
		0, 0, 0, 1, // partitionId
		3,            // 3-1=2 replica array size
		0, 0, 3, 233, // replica 1001
		0, 0, 3, 234, // replica 1002
		0, 0, // empty tagged fields
		0, // empty tagged fields
	}

	alterPartitionReassignmentsRequestTwoPartitions = []byte{
		0, 0, 39, 16, // timout 10000
		2,                         // 2-1=1 block
		6, 116, 111, 112, 105, 99, // topic name "topic" as compact string
		3,          // 3-1=2 partitions
		0, 0, 0, 0, // partitionId
		3,            // 3-1=2 replica array size
		0, 0, 3, 232, // replica 1000
		0, 0, 3, 233, // replica 1001
		0,          // empty tagged fields
		0, 0, 0, 1, // partitionId
		3,            // 3-1=2 replica array size
		0, 0, 3, 233, // replica 1001
		0, 0, 3, 234, // replica 1002
		0, 0, // empty tagged fields
		0, // empty tagged fields
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

	request.AddBlock("topic2", 1, []int32{1001, 1002})

	testRequest(t, "two blocks", request, alterPartitionReassignmentsRequestTwoBlocks)

	request = &AlterPartitionReassignmentsRequest{
		TimeoutMs: int32(10000),
		Version:   int16(0),
	}

	request.AddBlock("topic", 0, []int32{1000, 1001})
	request.AddBlock("topic", 1, []int32{1001, 1002})

	testRequest(t, "two partitions", request, alterPartitionReassignmentsRequestTwoPartitions)

}

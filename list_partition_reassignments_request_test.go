package sarama

import "testing"

var (
	listPartitionReassignmentsRequestOneBlock = []byte{
		0, 0, 39, 16, // timout 10000
		2,                         // 2-1=1 block
		6, 116, 111, 112, 105, 99, // topic name "topic" as compact string
		2,          // 2-1=1 partitions
		0, 0, 0, 0, // partitionId
		0, 0, // empty tagged fields
	}

	listPartitionReassignmentsRequestTwoBlocks = []byte{
		0, 0, 39, 16, // timout 10000
		3,                         // 3-1=2 blocks
		6, 116, 111, 112, 105, 99, // topic name "topic" as compact string
		2,          // 2-1=1 partitions
		0, 0, 0, 0, // partitionId
		0,                             // empty tagged fields
		7, 116, 111, 112, 105, 99, 50, // topic name "topic2" as compact string
		3,          // 3-1=2 partitions
		0, 0, 0, 1, // partitionId
		0, 0, 0, 2, // partitionId
		0, 0, // empty tagged fields
	}
)

func TestListPartitionReassignmentRequest(t *testing.T) {
	var request *ListPartitionReassignmentsRequest

	request = &ListPartitionReassignmentsRequest{
		TimeoutMs: int32(10000),
		Version:   int16(0),
	}

	request.AddBlock("topic", []int32{0})

	testRequest(t, "one block", request, listPartitionReassignmentsRequestOneBlock)

	request.AddBlock("topic2", []int32{1, 2})

	testRequest(t, "two blocks", request, listPartitionReassignmentsRequestTwoBlocks)
}

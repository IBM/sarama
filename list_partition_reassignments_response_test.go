package sarama

import "testing"

var (
	listPartitionReassignmentsResponse = []byte{
		0, 0, 39, 16, // ThrottleTimeMs 10000
		0, 0, // errorcode
		0,                         // null string
		2,                         // block array length 1
		6, 116, 111, 112, 105, 99, // topic name "topic"
		2,          // partition array length 1
		0, 0, 0, 1, // partitionId
		3, 0, 0, 3, 232, 0, 0, 3, 233, // replicas [1000, 1001]
		3, 0, 0, 3, 234, 0, 0, 3, 235, // addingReplicas [1002, 1003]
		3, 0, 0, 3, 236, 0, 0, 3, 237, // addingReplicas [1004, 1005]
		0, 0, 0, // empty tagged fields
	}
)

func TestListPartitionReassignmentResponse(t *testing.T) {
	var response *ListPartitionReassignmentsResponse

	response = &ListPartitionReassignmentsResponse{
		ThrottleTimeMs: int32(10000),
		Version:        int16(0),
	}

	response.AddBlock("topic", 1, []int32{1000, 1001}, []int32{1002, 1003}, []int32{1004, 1005})

	testResponse(t, "one topic", response, listPartitionReassignmentsResponse)
}

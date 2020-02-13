package sarama

import "testing"

var (
	alterPartitionReassignmentsResponseNoError = []byte{
		0, 0, 39, 16, // ThrottleTimeMs 10000
		0, 0, // errorcode
		0, // null string
		1, // empty errors array
		0, // empty tagged fields
	}
)

func TestAlterPartitionReassignmentResponse(t *testing.T) {
	var response *AlterPartitionReassignmentsResponse

	response = &AlterPartitionReassignmentsResponse{
		ThrottleTimeMs: int32(10000),
		Version:        int16(0),
	}

	testResponse(t, "no error", response, alterPartitionReassignmentsResponseNoError)
}

//go:build !functional

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

	alterPartitionReassignmentsResponseWithError = []byte{
		0, 0, 39, 16, // ThrottleTimeMs 10000
		0, 12, // errorcode
		6, 101, 114, 114, 111, 114, // error string "error"
		2,                         // errors array length 1
		6, 116, 111, 112, 105, 99, // topic name "topic"
		2,          // partition array length 1
		0, 0, 0, 1, // partitionId
		0, 3, // kerror
		7, 101, 114, 114, 111, 114, 50, // error string "error2"
		0, 0, 0, // empty tagged fields
	}
)

func TestAlterPartitionReassignmentResponse(t *testing.T) {
	var response *AlterPartitionReassignmentsResponse = &AlterPartitionReassignmentsResponse{
		ThrottleTimeMs: int32(10000),
		Version:        int16(0),
	}

	testResponse(t, "no error", response, alterPartitionReassignmentsResponseNoError)

	errorMessage := "error"
	partitionError := "error2"
	response.ErrorCode = 12
	response.ErrorMessage = &errorMessage
	response.AddError("topic", 1, 3, &partitionError)

	testResponse(t, "with error", response, alterPartitionReassignmentsResponseWithError)
}

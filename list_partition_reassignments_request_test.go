package sarama

import (
	"testing"

	"go.uber.org/goleak"
)

var listPartitionReassignmentsRequestOneBlock = []byte{
	0, 0, 39, 16, // timeout 10000
	2,                         // 2-1=1 block
	6, 116, 111, 112, 105, 99, // topic name "topic" as compact string
	2,          // 2-1=1 partitions
	0, 0, 0, 0, // partitionId
	0, 0, // empty tagged fields
}

func TestListPartitionReassignmentRequest(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	var request *ListPartitionReassignmentsRequest = &ListPartitionReassignmentsRequest{
		TimeoutMs: int32(10000),
		Version:   int16(0),
	}

	request.AddBlock("topic", []int32{0})

	testRequest(t, "one block", request, listPartitionReassignmentsRequestOneBlock)

	request.AddBlock("topic2", []int32{1, 2})

	testRequestWithoutByteComparison(t, "two blocks", request)
}

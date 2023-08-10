package sarama

import (
	"testing"

	"go.uber.org/goleak"
)

var (
	emptyDeleteOffsetsRequest = []byte{
		0, 3, 'f', 'o', 'o', // group name: foo
		0, 0, 0, 0, // 0 partition
	}
)

func TestDeleteOffsetsRequest(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	var request *DeleteOffsetsRequest

	request = new(DeleteOffsetsRequest)
	request.Group = "foo"

	testRequest(t, "no offset", request, emptyDeleteOffsetsRequest)

	request = new(DeleteOffsetsRequest)
	request.Group = "foo"
	request.AddPartition("bar", 6)
	request.AddPartition("bar", 7)
	// The response encoded form cannot be checked for it varies due to
	// unpredictable map traversal order.
	testRequest(t, "two offsets on one topic", request, nil)

	request = new(DeleteOffsetsRequest)
	request.Group = "foo"
	request.AddPartition("bar", 6)
	request.AddPartition("bar", 7)
	request.AddPartition("baz", 0)
	// The response encoded form cannot be checked for it varies due to
	// unpredictable map traversal order.
	testRequest(t, "three offsets on two topics", request, nil)
}

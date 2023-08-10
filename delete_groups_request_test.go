package sarama

import (
	"testing"

	"go.uber.org/goleak"
)

var (
	emptyDeleteGroupsRequest = []byte{0, 0, 0, 0}

	singleDeleteGroupsRequest = []byte{
		0, 0, 0, 1, // 1 group
		0, 3, 'f', 'o', 'o', // group name: foo
	}

	doubleDeleteGroupsRequest = []byte{
		0, 0, 0, 2, // 2 groups
		0, 3, 'f', 'o', 'o', // group name: foo
		0, 3, 'b', 'a', 'r', // group name: foo
	}
)

func TestDeleteGroupsRequest(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	var request *DeleteGroupsRequest

	request = new(DeleteGroupsRequest)
	testRequest(t, "no groups", request, emptyDeleteGroupsRequest)

	request = new(DeleteGroupsRequest)
	request.AddGroup("foo")
	testRequest(t, "one group", request, singleDeleteGroupsRequest)

	request = new(DeleteGroupsRequest)
	request.AddGroup("foo")
	request.AddGroup("bar")
	testRequest(t, "two groups", request, doubleDeleteGroupsRequest)
}

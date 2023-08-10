package sarama

import (
	"testing"

	"go.uber.org/goleak"
)

func TestListGroupsRequest(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	testRequest(t, "ListGroupsRequest", &ListGroupsRequest{}, []byte{})

	testRequest(t, "ListGroupsRequest", &ListGroupsRequest{
		Version: 1,
	}, []byte{})

	testRequest(t, "ListGroupsRequest", &ListGroupsRequest{
		Version: 2,
	}, []byte{})

	testRequest(t, "ListGroupsRequest", &ListGroupsRequest{
		Version: 3,
	}, []byte{
		0, //		0, // empty tag buffer
	})

	testRequest(t, "ListGroupsRequest", &ListGroupsRequest{
		Version: 4,
	}, []byte{
		1, // compact array length (0)
		0, // empty tag buffer
	})

	testRequest(t, "ListGroupsRequest", &ListGroupsRequest{
		Version:      4,
		StatesFilter: []string{"Empty"},
	}, []byte{
		2,                          // compact array length (1)
		6, 'E', 'm', 'p', 't', 'y', // compact string
		0, // empty tag buffer
	})
}

package sarama

import "testing"

func TestListGroupsRequest(t *testing.T) {
	t.Parallel()
	testRequest(t, "ListGroupsRequest", &ListGroupsRequest{}, []byte{})
}

//go:build !functional

package sarama

import "testing"

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

	emptyDeleteGroupsRequestV2 = []byte{
		1,
		0, // empty tagged fields
	}

	singleDeleteGroupsRequestV2 = []byte{
		2,                // 1 group
		4, 'f', 'o', 'o', // group name: foo
		0, // empty tagged fields
	}

	doubleDeleteGroupsRequestV2 = []byte{
		3,                // 2 groups
		4, 'f', 'o', 'o', // group name: foo
		4, 'b', 'a', 'r', // group name: foo
		0, // empty tagged fields
	}
)

func TestDeleteGroupsRequest(t *testing.T) {
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

func TestDeleteGroupsRequestV2(t *testing.T) {
	var request *DeleteGroupsRequest

	request = &DeleteGroupsRequest{
		Version: 2,
	}
	testRequest(t, "no groups", request, emptyDeleteGroupsRequestV2)

	request = &DeleteGroupsRequest{
		Version: 2,
	}
	request.AddGroup("foo")
	testRequest(t, "one group", request, singleDeleteGroupsRequestV2)

	request = &DeleteGroupsRequest{
		Version: 2,
	}
	request.AddGroup("foo")
	request.AddGroup("bar")
	testRequest(t, "two groups", request, doubleDeleteGroupsRequestV2)
}

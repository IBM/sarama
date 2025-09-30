//go:build !functional

package sarama

import "testing"

var (
	emptyDescribeGroupsRequest = []byte{0, 0, 0, 0}

	singleDescribeGroupsRequestV0 = []byte{
		0, 0, 0, 1, // 1 group
		0, 3, 'f', 'o', 'o', // group name: foo
	}

	doubleDescribeGroupsRequestV0 = []byte{
		0, 0, 0, 2, // 2 groups
		0, 3, 'f', 'o', 'o', // group name: foo
		0, 3, 'b', 'a', 'r', // group name: bar
	}
)

func TestDescribeGroupsRequestV0(t *testing.T) {
	var request *DescribeGroupsRequest

	request = new(DescribeGroupsRequest)
	testRequest(t, "no groups", request, emptyDescribeGroupsRequest)

	request = new(DescribeGroupsRequest)
	request.AddGroup("foo")
	testRequest(t, "one group", request, singleDescribeGroupsRequestV0)

	request = new(DescribeGroupsRequest)
	request.AddGroup("foo")
	request.AddGroup("bar")
	testRequest(t, "two groups", request, doubleDescribeGroupsRequestV0)
}

var (
	emptyDescribeGroupsRequestV3 = []byte{0, 0, 0, 0, 0}

	singleDescribeGroupsRequestV3 = []byte{
		0, 0, 0, 1, // 1 group
		0, 3, 'f', 'o', 'o', // group name: foo
		0,
	}

	doubleDescribeGroupsRequestV3 = []byte{
		0, 0, 0, 2, // 2 groups
		0, 3, 'f', 'o', 'o', // group name: foo
		0, 3, 'b', 'a', 'r', // group name: bar
		1,
	}
)

func TestDescribeGroupsRequestV3(t *testing.T) {
	var request *DescribeGroupsRequest

	request = new(DescribeGroupsRequest)
	request.Version = 3
	testRequest(t, "no groups", request, emptyDescribeGroupsRequestV3)

	request = new(DescribeGroupsRequest)
	request.Version = 3
	request.AddGroup("foo")
	testRequest(t, "one group", request, singleDescribeGroupsRequestV3)

	request = new(DescribeGroupsRequest)
	request.Version = 3
	request.AddGroup("foo")
	request.AddGroup("bar")
	request.IncludeAuthorizedOperations = true
	testRequest(t, "two groups", request, doubleDescribeGroupsRequestV3)
}

var (
	emptyDescribeGroupsRequestV5 = []byte{
		1, // 1+0 no groups
		0, // do not include authorized operations
		0, // empty tagged fields
	}

	singleDescribeGroupsRequestV5 = []byte{
		2,                // 1+1 group
		4, 'f', 'o', 'o', // group name: foo
		0, // do not include authorized operations
		0, // empty tagged fields
	}

	doubleDescribeGroupsRequestV5 = []byte{
		3,                // 1+2 groups
		4, 'f', 'o', 'o', // group name: foo
		4, 'b', 'a', 'r', // group name: bar
		1, // do include authorized operations
		0, // empty tagged fields
	}
)

func TestDescribeGroupsRequestV5(t *testing.T) {
	var request *DescribeGroupsRequest

	request = &DescribeGroupsRequest{Version: 5}
	testRequest(t, "no groups", request, emptyDescribeGroupsRequestV5)

	request = &DescribeGroupsRequest{Version: 5}
	request.AddGroup("foo")
	testRequest(t, "one group", request, singleDescribeGroupsRequestV5)

	request = &DescribeGroupsRequest{Version: 5}
	request.AddGroup("foo")
	request.AddGroup("bar")
	request.IncludeAuthorizedOperations = true
	testRequest(t, "two groups", request, doubleDescribeGroupsRequestV5)
}

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
		0, 3, 'b', 'a', 'r', // group name: foo
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
		0, 3, 'b', 'a', 'r', // group name: foo
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

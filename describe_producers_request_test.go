//go:build !functional

package sarama

import (
	"testing"
)

var describeProducersRequestV0 = []byte{
	2,                          // Topics
	6, 't', 'o', 'p', 'i', 'c', // name
	3,          // PartitionIndexes
	0, 0, 0, 0, // partition 0
	0, 0, 0, 1, // partition 1
	0, // empty tagged fields
	0, // empty tagged fields
}

func TestDescribeProducersRequest(t *testing.T) {
	request := &DescribeProducersRequest{
		Version: 0,
		Topics: []DescribeProducersRequestTopic{
			{
				Name:             "topic",
				PartitionIndexes: []int32{0, 1},
			},
		},
	}

	testRequest(t, "v0", request, describeProducersRequestV0)
}

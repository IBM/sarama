//go:build !functional

package sarama

import "testing"

var (
	emptyDescribeLogDirsRequest = []byte{255, 255, 255, 255} // Empty array (array length -1 sent)
	topicDescribeLogDirsRequest = []byte{
		0, 0, 0, 1, // DescribeTopics array, Array length 1
		0, 6, // Topic name length 6
		'r', 'a', 'n', 'd', 'o', 'm', // Topic name
		0, 0, 0, 2, // PartitionIDs int32 array, Array length 2
		0, 0, 0, 25, // PartitionID 25
		0, 0, 0, 26, // PartitionID 26
	}
	emptyDescribeLogDirsRequestV2 = []byte{0, 0} // Compact empty array length
	topicDescribeLogDirsRequestV2 = []byte{
		2,                            // DescribeTopics array, Array length 1+1
		7,                            // Topic name length 6+1
		'r', 'a', 'n', 'd', 'o', 'm', // Topic name
		3,           // PartitionIDs int32 array, Array length 2+1
		0, 0, 0, 25, // PartitionID 25
		0, 0, 0, 26, // PartitionID 26
		0, 0, // empty tagged fields
	}
)

func TestDescribeLogDirsRequest(t *testing.T) {
	request := &DescribeLogDirsRequest{
		Version:        0,
		DescribeTopics: []DescribeLogDirsRequestTopic{},
	}
	testRequest(t, "no topics", request, emptyDescribeLogDirsRequest)

	request.DescribeTopics = []DescribeLogDirsRequestTopic{
		{
			Topic:        "random",
			PartitionIDs: []int32{25, 26},
		},
	}
	testRequest(t, "with topics", request, topicDescribeLogDirsRequest)

	request = &DescribeLogDirsRequest{
		Version:        2,
		DescribeTopics: []DescribeLogDirsRequestTopic{},
	}
	testRequest(t, "no topics v2", request, emptyDescribeLogDirsRequestV2)

	request.DescribeTopics = []DescribeLogDirsRequestTopic{
		{
			Topic:        "random",
			PartitionIDs: []int32{25, 26},
		},
	}
	testRequest(t, "with topics v2", request, topicDescribeLogDirsRequestV2)
}

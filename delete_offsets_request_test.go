package sarama

import "testing"

var (
	emptyDeleteOffsetsRequest = []byte{
		0, 3, 'f', 'o', 'o', // group name: foo
		0, 0, 0, 0, // 0 partition
	}

	doubleDeleteOffsetsRequest = []byte{
		0, 3, 'f', 'o', 'o', // group name: foo
		0, 0, 0, 1, // 1 topic
		0, 3, 'b', 'a', 'r', // topic name: bar
		0, 0, 0, 2, // 2 partitions
		0, 0, 0, 6, // partition 6
		0, 0, 0, 7, // partition 7
	}

	tripleDeleteOffsetsRequest = []byte{
		0, 3, 'f', 'o', 'o', // group name: foo
		0, 0, 0, 2, // 2 topics
		0, 3, 'b', 'a', 'r', // topic name: bar
		0, 0, 0, 2, // 2 partitions
		0, 0, 0, 6, // partition 6
		0, 0, 0, 7, // partition 7
		0, 3, 'b', 'a', 'z', // topic name: baz
		0, 0, 0, 1, // 1 partition
		0, 0, 0, 0, // partition 0
	}
)

func TestDeleteOffsetsRequest(t *testing.T) {
	var request *DeleteOffsetsRequest

	request = new(DeleteOffsetsRequest)
	request.Group = "foo"

	testRequest(t, "no offset", request, emptyDeleteOffsetsRequest)

	request = new(DeleteOffsetsRequest)
	request.Group = "foo"
	request.AddPartition("bar", 6)
	request.AddPartition("bar", 7)
	testRequest(t, "two offsets on one topic", request, doubleDeleteOffsetsRequest)

	request = new(DeleteOffsetsRequest)
	request.Group = "foo"
	request.AddPartition("bar", 6)
	request.AddPartition("bar", 7)
	request.AddPartition("baz", 0)
	testRequest(t, "three offsets on two topics", request, tripleDeleteOffsetsRequest)
}

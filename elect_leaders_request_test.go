//go:build !functional

package sarama

import "testing"

var (
	electLeadersRequestOneTopicV1 = []byte{
		0,          // preferred election type
		0, 0, 0, 1, // 1 topic
		0, 5, 116, 111, 112, 105, 99, // topic name "topic" as compact string
		0, 0, 0, 1, // 1 partition
		0, 0, 0, 0, // partition 0
		0, 0, 39, 16, // timeout 10000
	}
	electLeadersRequestOneTopicV2 = []byte{
		0,                         // preferred election type
		2,                         // 2-1=1 topic
		6, 116, 111, 112, 105, 99, // topic name "topic" as compact string
		2,          // 2-1=1 partition
		0, 0, 0, 0, // partition 0
		0,            // empty tagged fields
		0, 0, 39, 16, // timeout 10000
		0, // empty tagged fields
	}
)

func TestElectLeadersRequest(t *testing.T) {
	var request = &ElectLeadersRequest{
		TimeoutMs: int32(10000),
		Version:   int16(1),
		TopicPartitions: map[string][]int32{
			"topic": {0},
		},
		Type: PreferredElection,
	}

	testRequest(t, "one topic V1", request, electLeadersRequestOneTopicV1)

	request.Version = 2
	testRequest(t, "one topic V2", request, electLeadersRequestOneTopicV2)
}

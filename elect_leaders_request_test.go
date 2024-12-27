//go:build !functional

package sarama

import "testing"

var electLeadersRequestOneTopic = []byte{
	0,                         // preferred election type
	2,                         // 2-1=1 topic
	6, 116, 111, 112, 105, 99, // topic name "topic" as compact string
	2,          // 2-1=1 partition
	0, 0, 0, 0, // partition 0
	0, 0, // empty tagged fields
	0, 39, 16, 0, // timeout 10000
}

func TestElectLeadersRequest(t *testing.T) {
	var request = &ElectLeadersRequest{
		TimeoutMs: int32(10000),
		Version:   int16(2),
		TopicPartitions: map[string][]int32{
			"topic": {0},
		},
		Type: PreferredElection,
	}

	testRequest(t, "one topic", request, electLeadersRequestOneTopic)
}

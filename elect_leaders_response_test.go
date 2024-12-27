//go:build !functional

package sarama

import "testing"

var electLeadersResponseOneTopic = []byte{
	0, 0, 3, 232, // ThrottleTimeMs 1000
	0, 0, // errorCode
	2,                         // number of topics
	6, 116, 111, 112, 105, 99, // topic name "topic"
	2,          // number of partitions
	0, 0, 0, 0, // partition 0
	0, 0, // empty tagged fields
	0, 0, // empty tagged fields
	0, 0, // empty tagged fields
}

func TestElectLeadersResponse(t *testing.T) {
	var response = &ElectLeadersResponse{
		Version:        int16(2),
		ThrottleTimeMs: int32(1000),
		ReplicaElectionResults: map[string]map[int32]*PartitionResult{
			"topic": {
				0: {},
			},
		},
	}

	testResponse(t, "one topic", response, electLeadersResponseOneTopic)
}

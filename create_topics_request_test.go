package sarama

import (
	"testing"
)

var (
	createTopicsRequestSingleTopicNoAssignNoConfig = []byte{
		0, 0, 0, 1,
		0, 1, 'a', // topic string
		0, 0, 0, 30, // num_partitions int32
		0, 3, // replication_factor
		0, 0, 0, 0, // 0 replica assignments
		0, 0, 0, 0, // 0 config values
		0, 0, 0, 0, // timeout int32
	}
)

func TestCreateTopicsRequest(t *testing.T) {
	reqs := new(CreateTopicsRequest)
	req := CreateTopicRequest{}
	req.Topic = "a"
	req.NumPartitions = 30
	req.ReplicationFactor = 3

	reqs.CreateRequests = append(reqs.CreateRequests, req)
	testRequest(t, "single topic no assignments no config", reqs, createTopicsRequestSingleTopicNoAssignNoConfig)
}

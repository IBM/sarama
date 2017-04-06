package sarama

import "testing"

var (
	createTopicsRequestNoTopics = []byte{
		// no requests
		0x00, 0x00, 0x00, 0x00,
		// no timeout
		0x00, 0x00, 0x00, 0x00,
	}

	createTopicsRequestTwoTopics = []byte{
		0x00, 0x00, 0x00, 0x02,
		// topic request 1

		0x00, 0x06, 't', 'o', 'p', 'i', 'c', '1',
		0x00, 0x00, 0x00, 0x02,
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04, 'k', 'e', 'y', '1',
		0x00, 0x06, 'v', 'a', 'l', 'u', 'e', '1',

		// topic request 2
		0x00, 0x06, 't', 'o', 'p', 'i', 'c', '2',
		0x00, 0x00, 0x00, 0x03,
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,

		// timeout
		0x00, 0x00, 0x00, 0x00,
	}
)

func TestCreateTopicsRequest(t *testing.T) {
	request := new(CreateTopicsRequest)
	testRequest(t, "no requests", request, createTopicsRequestNoTopics)

	request.Requests = []*CreateTopicRequest{{
		Topic:             "topic1",
		NumPartitions:     2,
		ReplicaAssignment: nil,
		Configs: map[string]string{
			"key1": "value1",
		}}, {
		Topic:             "topic2",
		NumPartitions:     3,
		ReplicaAssignment: nil,
	}}

	testRequest(t, "two topics", request, createTopicsRequestTwoTopics)
}

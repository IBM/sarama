package sarama

import "testing"

var (
	deleteTopicsRequestNoTopics = []byte{
		// no topics
		0x00, 0x00, 0x00, 0x00,
		// no timeout
		0x00, 0x00, 0x00, 0x00,
	}

	deleteTopicsRequestOneTopic = []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x06, 't', 'o', 'p', 'i', 'c', '1',
		0x00, 0x00, 0x00, 0x09,
	}
)

func TestDeleteTopicsRequest(t *testing.T) {
	request := new(DeleteTopicsRequest)
	testRequest(t, "no requests", request, deleteTopicsRequestNoTopics)

	request.Topics = []string{"topic1"}
	request.Timeout = 0x09

	testRequest(t, "one topic", request, deleteTopicsRequestOneTopic)
}

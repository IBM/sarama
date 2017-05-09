package sarama

import "testing"

var (
	metadataRequestNoTopics = []byte{
		0x00, 0x00, 0x00, 0x00}

	metadataRequestOneTopic = []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x06, 't', 'o', 'p', 'i', 'c', '1'}

	metadataRequestThreeTopics = []byte{
		0x00, 0x00, 0x00, 0x03,
		0x00, 0x03, 'f', 'o', 'o',
		0x00, 0x03, 'b', 'a', 'r',
		0x00, 0x03, 'b', 'a', 'z'}

	metadataRequestV1AllTopics = []byte{
		0xFF, 0xFF, 0xFF, 0xFF,
	}
)

func TestMetadataRequest(t *testing.T) {
	request := &MetadataRequest{Version: 0}
	testRequest(t, "no topics", request, metadataRequestNoTopics)

	request.Topics = []string{"topic1"}
	testRequest(t, "one topic", request, metadataRequestOneTopic)

	request.Topics = []string{"foo", "bar", "baz"}
	testRequest(t, "three topics", request, metadataRequestThreeTopics)
}

func TestMetadataRequestV1(t *testing.T) {
	request := &MetadataRequest{Version: 1}
	testRequest(t, "no topics", request, metadataRequestNoTopics)

	request.AllTopics = true
	testRequest(t, "all topics", request, metadataRequestV1AllTopics)

	request.AllTopics = false
	request.Topics = []string{"topic1"}
	testRequest(t, "one topic", request, metadataRequestOneTopic)

	request.Topics = []string{"foo", "bar", "baz"}
	testRequest(t, "three topics", request, metadataRequestThreeTopics)
}

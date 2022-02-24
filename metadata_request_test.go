package sarama

import "testing"

var (
	// The v0 metadata request has a non-nullable array of topic names
	// to request metadata for. An empty array fetches metadata for all topics

	metadataRequestNoTopicsV0 = []byte{
		0x00, 0x00, 0x00, 0x00,
	}

	metadataRequestOneTopicV0 = []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x06, 't', 'o', 'p', 'i', 'c', '1',
	}

	metadataRequestThreeTopicsV0 = []byte{
		0x00, 0x00, 0x00, 0x03,
		0x00, 0x03, 'f', 'o', 'o',
		0x00, 0x03, 'b', 'a', 'r',
		0x00, 0x03, 'b', 'a', 'z',
	}

	// The v1 metadata request is the same as v0 except that the array is now
	// nullable and should be explicitly null if all topics are required (an
	// empty list requests no topics)

	metadataRequestNoTopicsV1 = []byte{
		0xff, 0xff, 0xff, 0xff,
	}

	metadataRequestOneTopicV1    = metadataRequestOneTopicV0
	metadataRequestThreeTopicsV1 = metadataRequestThreeTopicsV0

	// The v2 metadata request is the same as v1. An additional field for
	// cluster id has been added to the v2 metadata response

	metadataRequestNoTopicsV2    = metadataRequestNoTopicsV1
	metadataRequestOneTopicV2    = metadataRequestOneTopicV1
	metadataRequestThreeTopicsV2 = metadataRequestThreeTopicsV1

	// The v3 metadata request is the same as v1 and v2. An additional field
	// for throttle time has been added to the v3 metadata response

	metadataRequestNoTopicsV3    = metadataRequestNoTopicsV2
	metadataRequestOneTopicV3    = metadataRequestOneTopicV2
	metadataRequestThreeTopicsV3 = metadataRequestThreeTopicsV2

	// The v4 metadata request has an additional field for allowing auto topic
	// creation. The response is the same as v3.

	metadataRequestNoTopicsV4     = append(metadataRequestNoTopicsV1, byte(0))
	metadataRequestAutoCreateV4   = append(metadataRequestOneTopicV3, byte(1))
	metadataRequestNoAutoCreateV4 = append(metadataRequestOneTopicV3, byte(0))

	// The v5 metadata request is the same as v4. An additional field for
	// offline_replicas has been added to the v5 metadata response

	metadataRequestNoTopicsV5     = append(metadataRequestNoTopicsV1, byte(0))
	metadataRequestAutoCreateV5   = append(metadataRequestOneTopicV3, byte(1))
	metadataRequestNoAutoCreateV5 = append(metadataRequestOneTopicV3, byte(0))
)

func TestMetadataRequestV0(t *testing.T) {
	request := new(MetadataRequest)
	testRequest(t, "no topics", request, metadataRequestNoTopicsV0)

	request.Topics = []string{"topic1"}
	testRequest(t, "one topic", request, metadataRequestOneTopicV0)

	request.Topics = []string{"foo", "bar", "baz"}
	testRequest(t, "three topics", request, metadataRequestThreeTopicsV0)
}

func TestMetadataRequestV1(t *testing.T) {
	request := new(MetadataRequest)
	request.Version = 1
	testRequest(t, "no topics", request, metadataRequestNoTopicsV1)

	request.Topics = []string{"topic1"}
	testRequest(t, "one topic", request, metadataRequestOneTopicV1)

	request.Topics = []string{"foo", "bar", "baz"}
	testRequest(t, "three topics", request, metadataRequestThreeTopicsV1)
}

func TestMetadataRequestV2(t *testing.T) {
	request := new(MetadataRequest)
	request.Version = 2
	testRequest(t, "no topics", request, metadataRequestNoTopicsV2)

	request.Topics = []string{"topic1"}
	testRequest(t, "one topic", request, metadataRequestOneTopicV2)

	request.Topics = []string{"foo", "bar", "baz"}
	testRequest(t, "three topics", request, metadataRequestThreeTopicsV2)
}

func TestMetadataRequestV3(t *testing.T) {
	request := new(MetadataRequest)
	request.Version = 3
	testRequest(t, "no topics", request, metadataRequestNoTopicsV3)

	request.Topics = []string{"topic1"}
	testRequest(t, "one topic", request, metadataRequestOneTopicV3)

	request.Topics = []string{"foo", "bar", "baz"}
	testRequest(t, "three topics", request, metadataRequestThreeTopicsV3)
}

func TestMetadataRequestV4(t *testing.T) {
	request := new(MetadataRequest)
	request.Version = 4
	testRequest(t, "no topics", request, metadataRequestNoTopicsV4)

	request.Topics = []string{"topic1"}

	request.AllowAutoTopicCreation = true
	testRequest(t, "one topic", request, metadataRequestAutoCreateV4)

	request.AllowAutoTopicCreation = false
	testRequest(t, "one topic", request, metadataRequestNoAutoCreateV4)
}

func TestMetadataRequestV5(t *testing.T) {
	request := new(MetadataRequest)
	request.Version = 5
	testRequest(t, "no topics", request, metadataRequestNoTopicsV5)

	request.Topics = []string{"topic1"}

	request.AllowAutoTopicCreation = true
	testRequest(t, "one topic", request, metadataRequestAutoCreateV5)

	request.AllowAutoTopicCreation = false
	testRequest(t, "one topic", request, metadataRequestNoAutoCreateV5)
}

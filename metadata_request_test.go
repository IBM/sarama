//go:build !functional

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

	// The v6 metadata request and response are the same as v5. I know, right.
	metadataRequestNoTopicsV6     = metadataRequestNoTopicsV5
	metadataRequestAutoCreateV6   = metadataRequestAutoCreateV5
	metadataRequestNoAutoCreateV6 = metadataRequestNoAutoCreateV5

	// The v7 metadata request is the same as v6. An additional field for
	// leader epoch has been added to the partition metadata in the v7 response.
	metadataRequestNoTopicsV7     = metadataRequestNoTopicsV6
	metadataRequestAutoCreateV7   = metadataRequestAutoCreateV6
	metadataRequestNoAutoCreateV7 = metadataRequestNoAutoCreateV6

	// The v8 metadata request has additional fields for including cluster authorized operations
	// and including topic authorized operations. An additional field for cluster authorized operations
	// has been added to the v8 metadata response, and an additional field for topic authorized operations
	// has been added to the topic metadata in the v8 metadata response.
	metadataRequestNoTopicsV8     = append(metadataRequestNoTopicsV7, []byte{0, 0}...)
	metadataRequestAutoCreateV8   = append(metadataRequestAutoCreateV7, []byte{0, 0}...)
	metadataRequestNoAutoCreateV8 = append(metadataRequestNoAutoCreateV7, []byte{0, 0}...)
	// Appending to an empty slice means we are creating a new backing array, rather than updating the backing array
	// for the slice metadataRequestAutoCreateV7
	metadataRequestAutoCreateClusterAuthTopicAuthV8 = append(append([]byte{}, metadataRequestAutoCreateV7...), []byte{1, 1}...)

	// In v9 tag buffers have been added to the end of arrays, and various types have been replaced with compact types.
	metadataRequestNoTopicsV9 = []byte{
		0x00, 0x00, 0x00, 0x00, 0x00,
	}

	metadataRequestOneTopicV9 = []byte{
		2, 7, 't', 'o', 'p', 'i', 'c', '1', 0, 0, 0, 0, 0,
	}

	metadataRequestOneTopicAutoCreateTopicV9 = []byte{
		2, 7, 't', 'o', 'p', 'i', 'c', '1', 0, 1, 0, 1, 0,
	}

	// v10 added topic UUIDs to the metadata request and responses, and made the topic name nullable in the request.
	metadataRequestNoTopicsV10 = metadataRequestNoTopicsV9

	metadataRequestTwoTopicsV10 = []byte{
		3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 't', 'o', 'p', 'i', 'c', '1',
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 't', 'o', 'p', 'i', 'c', '2', 0, 0, 0, 0, 0,
	}

	metadataRequestAutoCreateClusterAuthTopicAuthV10 = []byte{
		3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 't', 'o', 'p', 'i', 'c', '1',
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 't', 'o', 'p', 'i', 'c', '2', 0, 1, 1, 1, 0,
	}
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

func TestMetadataRequestV6(t *testing.T) {
	request := new(MetadataRequest)
	request.Version = 6
	testRequest(t, "no topics", request, metadataRequestNoTopicsV6)

	request.Topics = []string{"topic1"}

	request.AllowAutoTopicCreation = true
	testRequest(t, "one topic", request, metadataRequestAutoCreateV6)

	request.AllowAutoTopicCreation = false
	testRequest(t, "one topic", request, metadataRequestNoAutoCreateV6)
}

func TestMetadataRequestV7(t *testing.T) {
	request := new(MetadataRequest)
	request.Version = 7
	testRequest(t, "no topics", request, metadataRequestNoTopicsV7)

	request.Topics = []string{"topic1"}

	request.AllowAutoTopicCreation = true
	testRequest(t, "one topic", request, metadataRequestAutoCreateV7)

	request.AllowAutoTopicCreation = false
	testRequest(t, "one topic", request, metadataRequestNoAutoCreateV7)
}

func TestMetadataRequestV8(t *testing.T) {
	request := new(MetadataRequest)
	request.Version = 8
	testRequest(t, "no topics", request, metadataRequestNoTopicsV8)

	request.Topics = []string{"topic1"}

	request.AllowAutoTopicCreation = true
	testRequest(t, "one topic, auto create", request, metadataRequestAutoCreateV8)

	request.AllowAutoTopicCreation = false
	testRequest(t, "one topic, no auto create", request, metadataRequestNoAutoCreateV8)

	request.AllowAutoTopicCreation = true
	request.IncludeClusterAuthorizedOperations = true
	request.IncludeTopicAuthorizedOperations = true
	testRequest(t, "one topic, auto create, cluster auth, topic auth", request, metadataRequestAutoCreateClusterAuthTopicAuthV8)
}

func TestMetadataRequestV9(t *testing.T) {
	request := new(MetadataRequest)
	request.Version = 9
	testRequest(t, "no topics", request, metadataRequestNoTopicsV9)

	request.Topics = []string{"topic1"}
	testRequest(t, "one topic", request, metadataRequestOneTopicV9)

	request.AllowAutoTopicCreation = true
	request.IncludeTopicAuthorizedOperations = true
	testRequest(t, "one topic, auto create, no cluster auth, topic auth", request, metadataRequestOneTopicAutoCreateTopicV9)
}

func TestMetadataRequestV10(t *testing.T) {
	request := new(MetadataRequest)
	request.Version = 10
	testRequest(t, "no topics", request, metadataRequestNoTopicsV10)

	request.Topics = []string{"topic1", "topic2"}
	testRequest(t, "one topic", request, metadataRequestTwoTopicsV10)

	request.AllowAutoTopicCreation = true
	request.IncludeClusterAuthorizedOperations = true
	request.IncludeTopicAuthorizedOperations = true
	testRequest(t, "one topic, auto create, cluster auth, topic auth", request, metadataRequestAutoCreateClusterAuthTopicAuthV10)
}

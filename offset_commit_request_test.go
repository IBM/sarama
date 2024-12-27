//go:build !functional

package sarama

import (
	"fmt"
	"reflect"
	"testing"
)

var (
	offsetCommitRequestNoBlocksV0 = []byte{
		0x00, 0x06, 'f', 'o', 'o', 'b', 'a', 'r',
		0x00, 0x00, 0x00, 0x00,
	}

	offsetCommitRequestNoBlocksV1 = []byte{
		0x00, 0x06, 'f', 'o', 'o', 'b', 'a', 'r',
		0x00, 0x00, 0x11, 0x22,
		0x00, 0x04, 'c', 'o', 'n', 's',
		0x00, 0x00, 0x00, 0x00,
	}

	offsetCommitRequestNoBlocksV2 = []byte{
		0x00, 0x06, 'f', 'o', 'o', 'b', 'a', 'r',
		0x00, 0x00, 0x11, 0x22,
		0x00, 0x04, 'c', 'o', 'n', 's',
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x33,
		0x00, 0x00, 0x00, 0x00,
	}

	offsetCommitRequestOneBlockV0 = []byte{
		0x00, 0x06, 'f', 'o', 'o', 'b', 'a', 'r',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x05, 't', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x52, 0x21,
		0x00, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF,
		0x00, 0x08, 'm', 'e', 't', 'a', 'd', 'a', 't', 'a',
	}

	offsetCommitRequestOneBlockV1 = []byte{
		0x00, 0x06, 'f', 'o', 'o', 'b', 'a', 'r',
		0x00, 0x00, 0x11, 0x22,
		0x00, 0x04, 'c', 'o', 'n', 's',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x05, 't', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x52, 0x21,
		0x00, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0x00, 0x08, 'm', 'e', 't', 'a', 'd', 'a', 't', 'a',
	}

	offsetCommitRequestOneBlockV2 = []byte{
		0x00, 0x06, 'f', 'o', 'o', 'b', 'a', 'r',
		0x00, 0x00, 0x11, 0x22,
		0x00, 0x04, 'c', 'o', 'n', 's',
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x33,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x05, 't', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x52, 0x21,
		0x00, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF,
		0x00, 0x08, 'm', 'e', 't', 'a', 'd', 'a', 't', 'a',
	}
)

func TestOffsetCommitRequestV0(t *testing.T) {
	request := new(OffsetCommitRequest)
	request.Version = 0
	request.ConsumerGroup = "foobar"
	testRequest(t, "no blocks v0", request, offsetCommitRequestNoBlocksV0)

	request.AddBlock("topic", 0x5221, 0xDEADBEEF, 0, "metadata")
	testRequest(t, "one block v0", request, offsetCommitRequestOneBlockV0)
}

func TestOffsetCommitRequestV1(t *testing.T) {
	request := new(OffsetCommitRequest)
	request.ConsumerGroup = "foobar"
	request.ConsumerID = "cons"
	request.ConsumerGroupGeneration = 0x1122
	request.Version = 1
	testRequest(t, "no blocks v1", request, offsetCommitRequestNoBlocksV1)

	request.AddBlock("topic", 0x5221, 0xDEADBEEF, ReceiveTime, "metadata")
	testRequest(t, "one block v1", request, offsetCommitRequestOneBlockV1)
}

func TestOffsetCommitRequestV2ToV4(t *testing.T) {
	for version := 2; version <= 4; version++ {
		request := new(OffsetCommitRequest)
		request.ConsumerGroup = "foobar"
		request.ConsumerID = "cons"
		request.ConsumerGroupGeneration = 0x1122
		request.RetentionTime = 0x4433
		request.Version = int16(version)
		testRequest(t, fmt.Sprintf("no blocks v%d", version), request, offsetCommitRequestNoBlocksV2)

		request.AddBlock("topic", 0x5221, 0xDEADBEEF, 0, "metadata")
		testRequest(t, fmt.Sprintf("one block v%d", version), request, offsetCommitRequestOneBlockV2)
	}
}

var (
	offsetCommitRequestOneBlockV5 = []byte{
		0, 3, 'f', 'o', 'o', // GroupId
		0x00, 0x00, 0x00, 0x01, // GenerationId
		0, 3, 'm', 'i', 'd', // MemberId
		0, 0, 0, 1, // One Topic
		0, 5, 't', 'o', 'p', 'i', 'c', // Name
		0, 0, 0, 1, // One Partition
		0, 0, 0, 1, // PartitionIndex
		0, 0, 0, 0, 0, 0, 0, 2, // CommittedOffset
		0, 4, 'm', 'e', 't', 'a', // CommittedMetadata
	}
	offsetCommitRequestOneBlockV6 = []byte{
		0, 3, 'f', 'o', 'o', // GroupId
		0x00, 0x00, 0x00, 0x01, // GenerationId
		0, 3, 'm', 'i', 'd', // MemberId
		0, 0, 0, 1, // One Topic
		0, 5, 't', 'o', 'p', 'i', 'c', // Name
		0, 0, 0, 1, // One Partition
		0, 0, 0, 1, // PartitionIndex
		0, 0, 0, 0, 0, 0, 0, 2, // CommittedOffset
		0, 0, 0, 3, // CommittedEpoch
		0, 4, 'm', 'e', 't', 'a', // CommittedMetadata
	}
	offsetCommitRequestOneBlockV7 = []byte{
		0, 3, 'f', 'o', 'o', // GroupId
		0x00, 0x00, 0x00, 0x01, // GenerationId
		0, 3, 'm', 'i', 'd', // MemberId
		0, 3, 'g', 'i', 'd', // MemberId
		0, 0, 0, 1, // One Topic
		0, 5, 't', 'o', 'p', 'i', 'c', // Name
		0, 0, 0, 1, // One Partition
		0, 0, 0, 1, // PartitionIndex
		0, 0, 0, 0, 0, 0, 0, 2, // CommittedOffset
		0, 0, 0, 3, // CommittedEpoch
		0, 4, 'm', 'e', 't', 'a', // CommittedMetadata
	}
)

func TestOffsetCommitRequestV5AndPlus(t *testing.T) {
	groupInstanceId := "gid"
	tests := []struct {
		CaseName     string
		Version      int16
		MessageBytes []byte
		Message      *OffsetCommitRequest
	}{
		{
			"v5",
			5,
			offsetCommitRequestOneBlockV5,
			&OffsetCommitRequest{
				Version:                 5,
				ConsumerGroup:           "foo",
				ConsumerGroupGeneration: 1,
				ConsumerID:              "mid",
				blocks: map[string]map[int32]*offsetCommitRequestBlock{
					"topic": {
						1: &offsetCommitRequestBlock{offset: 2, metadata: "meta"},
					},
				},
			},
		},
		{
			"v6",
			6,
			offsetCommitRequestOneBlockV6,
			&OffsetCommitRequest{
				Version:                 6,
				ConsumerGroup:           "foo",
				ConsumerGroupGeneration: 1,
				ConsumerID:              "mid",
				blocks: map[string]map[int32]*offsetCommitRequestBlock{
					"topic": {
						1: &offsetCommitRequestBlock{offset: 2, metadata: "meta", committedLeaderEpoch: 3},
					},
				},
			},
		},
		{
			"v7",
			7,
			offsetCommitRequestOneBlockV7,
			&OffsetCommitRequest{
				Version:                 7,
				ConsumerGroup:           "foo",
				ConsumerGroupGeneration: 1,
				ConsumerID:              "mid",
				GroupInstanceId:         &groupInstanceId,
				blocks: map[string]map[int32]*offsetCommitRequestBlock{
					"topic": {
						1: &offsetCommitRequestBlock{offset: 2, metadata: "meta", committedLeaderEpoch: 3},
					},
				},
			},
		},
	}
	for _, c := range tests {
		request := new(OffsetCommitRequest)
		testVersionDecodable(t, c.CaseName, request, c.MessageBytes, c.Version)
		if !reflect.DeepEqual(c.Message, request) {
			t.Errorf("case %s decode failed, expected:%+v got %+v", c.CaseName, c.Message, request)
		}
		testEncodable(t, c.CaseName, c.Message, c.MessageBytes)
	}
}

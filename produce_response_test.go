//go:build !functional

package sarama

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	produceResponseNoBlocksV0 = []byte{
		0x00, 0x00, 0x00, 0x00,
	}

	produceResponseManyBlocksVersions = map[int][]byte{
		0: {
			0x00, 0x00, 0x00, 0x01,

			0x00, 0x03, 'f', 'o', 'o',
			0x00, 0x00, 0x00, 0x01,

			0x00, 0x00, 0x00, 0x01, // Partition 1
			0x00, 0x02, // ErrInvalidMessage
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, // Offset 255
		},

		1: {
			0x00, 0x00, 0x00, 0x01,

			0x00, 0x03, 'f', 'o', 'o',
			0x00, 0x00, 0x00, 0x01,

			0x00, 0x00, 0x00, 0x01, // Partition 1
			0x00, 0x02, // ErrInvalidMessage
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, // Offset 255

			0x00, 0x00, 0x00, 0x64, // 100 ms throttle time
		},
		2: {
			0x00, 0x00, 0x00, 0x01,

			0x00, 0x03, 'f', 'o', 'o',
			0x00, 0x00, 0x00, 0x01,

			0x00, 0x00, 0x00, 0x01, // Partition 1
			0x00, 0x02, // ErrInvalidMessage
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, // Offset 255
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8, // Timestamp January 1st 0001 at 00:00:01,000 UTC (LogAppendTime was used)

			0x00, 0x00, 0x00, 0x64, // 100 ms throttle time
		},
		7: { // version 7 adds StartOffset
			0x00, 0x00, 0x00, 0x01,

			0x00, 0x03, 'f', 'o', 'o',
			0x00, 0x00, 0x00, 0x01,

			0x00, 0x00, 0x00, 0x01, // Partition 1
			0x00, 0x02, // ErrInvalidMessage
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, // Offset 255
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8, // Timestamp January 1st 0001 at 00:00:01,000 UTC (LogAppendTime was used)
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, // StartOffset 50

			0x00, 0x00, 0x00, 0x64, // 100 ms throttle time
		},
		8: { // version 8 adds RecordErrors and ErrorMessage (KIP-467)
			0x00, 0x00, 0x00, 0x01,

			0x00, 0x03, 'f', 'o', 'o',
			0x00, 0x00, 0x00, 0x01,

			0x00, 0x00, 0x00, 0x01, // Partition 1
			0x00, 0x02, // ErrInvalidMessage
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, // Offset 255
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8, // Timestamp January 1st 0001 at 00:00:01,000 UTC (LogAppendTime was used)
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, // StartOffset 50
			0x00, 0x00, 0x00, 0x01, // 1 record error
			0x00, 0x00, 0x00, 0x03, // BatchIndex 3
			0x00, 0x07, 'b', 'a', 'd', ' ', 'r', 'e', 'c', // BatchIndexErrorMessage "bad rec"
			0x00, 0x09, 'b', 'a', 'd', ' ', 'b', 'a', 't', 'c', 'h', // ErrorMessage "bad batch"

			0x00, 0x00, 0x00, 0x64, // 100 ms throttle time
		},
		9: { // version 9 is the first flexible version
			0x02, // 1 topic

			0x04, 'f', 'o', 'o',
			0x02, // 1 partition

			0x00, 0x00, 0x00, 0x01, // Partition 1
			0x00, 0x02, // ErrInvalidMessage
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, // Offset 255
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8, // Timestamp January 1st 0001 at 00:00:01,000 UTC (LogAppendTime was used)
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, // StartOffset 50
			0x02,                   // 1 record error
			0x00, 0x00, 0x00, 0x03, // BatchIndex 3
			0x08, 'b', 'a', 'd', ' ', 'r', 'e', 'c', // BatchIndexErrorMessage
			0x00,                                              // record error tagged fields
			0x0A, 'b', 'a', 'd', ' ', 'b', 'a', 't', 'c', 'h', // ErrorMessage
			0x00, // partition tagged fields
			0x00, // topic tagged fields

			0x00, 0x00, 0x00, 0x64, // 100 ms throttle time
			0x00, // response tagged fields
		},
		10: { // version 10 adds CurrentLeader and NodeEndpoints tagged fields (KIP-951), absent here
			0x02, // 1 topic

			0x04, 'f', 'o', 'o',
			0x02, // 1 partition

			0x00, 0x00, 0x00, 0x01, // Partition 1
			0x00, 0x02, // ErrInvalidMessage
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, // Offset 255
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8, // Timestamp January 1st 0001 at 00:00:01,000 UTC (LogAppendTime was used)
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, // StartOffset 50
			0x02,                   // 1 record error
			0x00, 0x00, 0x00, 0x03, // BatchIndex 3
			0x08, 'b', 'a', 'd', ' ', 'r', 'e', 'c', // BatchIndexErrorMessage
			0x00,                                              // record error tagged fields
			0x0A, 'b', 'a', 'd', ' ', 'b', 'a', 't', 'c', 'h', // ErrorMessage
			0x00, // partition tagged fields
			0x00, // topic tagged fields

			0x00, 0x00, 0x00, 0x64, // 100 ms throttle time
			0x00, // response tagged fields
		},
	}

	// produceResponseKIP951V10 carries a NOT_LEADER_OR_FOLLOWER partition with a
	// populated CurrentLeader hint and the matching NodeEndpoints (KIP-951).
	produceResponseKIP951V10 = []byte{
		0x02, // 1 topic

		0x04, 'f', 'o', 'o',
		0x02, // 1 partition

		0x00, 0x00, 0x00, 0x01, // Partition 1
		0x00, 0x06, // ErrNotLeaderForPartition
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, // Offset 255
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // Timestamp -1
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, // StartOffset 50
		0x01, // 0 record errors
		0x00, // ErrorMessage null
		// partition tagged fields
		0x01,                   // 1 tagged field
		0x00,                   // tag 0 (CurrentLeader)
		0x09,                   // size
		0x00, 0x00, 0x00, 0x05, // LeaderID 5
		0x00, 0x00, 0x00, 0x07, // LeaderEpoch 7
		0x00, // CurrentLeader tagged fields
		0x00, // topic tagged fields

		0x00, 0x00, 0x00, 0x64, // 100 ms throttle time
		// response tagged fields
		0x01,                   // 1 tagged field
		0x00,                   // tag 0 (NodeEndpoints)
		0x0D,                   // size
		0x02,                   // 1 node endpoint
		0x00, 0x00, 0x00, 0x05, // NodeID 5
		0x02, 'h', // Host "h"
		0x00, 0x00, 0x23, 0x84, // Port 9092
		0x00, // Rack null
		0x00, // node endpoint tagged fields
	}
)

func TestProduceResponseDecode(t *testing.T) {
	response := ProduceResponse{}

	testVersionDecodable(t, "no blocks", &response, produceResponseNoBlocksV0, 0)
	if len(response.Blocks) != 0 {
		t.Error("Decoding produced", len(response.Blocks), "topics where there were none")
	}

	for v, produceResponseManyBlocks := range produceResponseManyBlocksVersions {
		t.Logf("Decoding produceResponseManyBlocks version %d", v)
		testVersionDecodable(t, "many blocks", &response, produceResponseManyBlocks, int16(v))
		if len(response.Blocks) != 1 {
			t.Error("Decoding produced", len(response.Blocks), "topics where there was 1")
		}
		if len(response.Blocks["foo"]) != 1 {
			t.Error("Decoding produced", len(response.Blocks["foo"]), "partitions for 'foo' where there was one")
		}
		block := response.GetBlock("foo", 1)
		if block == nil {
			t.Error("Decoding did not produce a block for foo/1")
		} else {
			if !errors.Is(block.Err, ErrInvalidMessage) {
				t.Error("Decoding failed for foo/1/Err, got:", int16(block.Err))
			}
			if block.Offset != 255 {
				t.Error("Decoding failed for foo/1/Offset, got:", block.Offset)
			}
			if v >= 2 {
				if !block.Timestamp.Equal(time.Unix(1, 0)) {
					t.Error("Decoding failed for foo/1/Timestamp, got:", block.Timestamp)
				}
			}
			if v >= 7 {
				if block.StartOffset != 50 {
					t.Error("Decoding failed for foo/1/StartOffset, got:", block.StartOffset)
				}
			}
			if v >= 8 {
				if len(block.RecordErrors) != 1 {
					t.Error("Decoding failed for foo/1/RecordErrors, expected 1 entry, got:", len(block.RecordErrors))
				} else {
					if block.RecordErrors[0].BatchIndex != 3 {
						t.Error("Decoding failed for foo/1/RecordErrors[0].BatchIndex, got:", block.RecordErrors[0].BatchIndex)
					}
					if block.RecordErrors[0].BatchIndexErrorMessage == nil || *block.RecordErrors[0].BatchIndexErrorMessage != "bad rec" {
						t.Error("Decoding failed for foo/1/RecordErrors[0].BatchIndexErrorMessage, got:", block.RecordErrors[0].BatchIndexErrorMessage)
					}
				}
				if block.ErrorMessage == nil || *block.ErrorMessage != "bad batch" {
					t.Error("Decoding failed for foo/1/ErrorMessage, got:", block.ErrorMessage)
				}
			}
		}
		if v >= 1 {
			if expected := 100 * time.Millisecond; response.ThrottleTime != expected {
				t.Error("Failed decoding produced throttle time, expected:", expected, ", got:", response.ThrottleTime)
			}
		}
	}
}

func TestProduceResponseEncode(t *testing.T) {
	response := ProduceResponse{}
	response.Blocks = make(map[string]map[int32]*ProduceResponseBlock)
	testEncodable(t, "empty", &response, produceResponseNoBlocksV0)

	batchIndexErrMsg := "bad rec"
	errMsg := "bad batch"
	response.Blocks["foo"] = make(map[int32]*ProduceResponseBlock)
	response.Blocks["foo"][1] = &ProduceResponseBlock{
		Err:         ErrInvalidMessage,
		Offset:      255,
		Timestamp:   time.Unix(1, 0),
		StartOffset: 50,
		RecordErrors: []ProduceResponseRecordError{{
			BatchIndex:             3,
			BatchIndexErrorMessage: &batchIndexErrMsg,
		}},
		ErrorMessage: &errMsg,
	}
	response.ThrottleTime = 100 * time.Millisecond
	for v, produceResponseManyBlocks := range produceResponseManyBlocksVersions {
		response.Version = int16(v)
		testEncodable(t, fmt.Sprintf("many blocks version %d", v), &response, produceResponseManyBlocks)
	}
}

func TestProduceResponseV10(t *testing.T) {
	response := ProduceResponse{Version: 10}
	response.Blocks = map[string]map[int32]*ProduceResponseBlock{
		"foo": {
			1: {
				Err:           ErrNotLeaderForPartition,
				Offset:        255,
				StartOffset:   50,
				CurrentLeader: &ProduceResponseCurrentLeader{LeaderID: 5, LeaderEpoch: 7},
			},
		},
	}
	response.ThrottleTime = 100 * time.Millisecond
	response.NodeEndpoints = []*ProduceResponseNodeEndpoint{
		{NodeID: 5, Host: "h", Port: 9092},
	}

	testEncodable(t, "current leader and node endpoints", &response, produceResponseKIP951V10)

	decoded := ProduceResponse{}
	testVersionDecodable(t, "current leader and node endpoints", &decoded, produceResponseKIP951V10, 10)

	block := decoded.GetBlock("foo", 1)
	require.NotNil(t, block)
	require.NotNil(t, block.CurrentLeader)
	assert.Equal(t, int32(5), block.CurrentLeader.LeaderID)
	assert.Equal(t, int32(7), block.CurrentLeader.LeaderEpoch)

	require.Len(t, decoded.NodeEndpoints, 1)
	assert.Equal(t, int32(5), decoded.NodeEndpoints[0].NodeID)
	assert.Equal(t, "h", decoded.NodeEndpoints[0].Host)
	assert.Equal(t, int32(9092), decoded.NodeEndpoints[0].Port)
	assert.Nil(t, decoded.NodeEndpoints[0].Rack)
}

func TestProduceResponseEncodeInvalidTimestamp(t *testing.T) {
	response := ProduceResponse{}
	response.Version = 2
	response.Blocks = make(map[string]map[int32]*ProduceResponseBlock)
	response.Blocks["t"] = make(map[int32]*ProduceResponseBlock)
	response.Blocks["t"][0] = &ProduceResponseBlock{
		Err:    ErrNoError,
		Offset: 0,
		// Use a timestamp before Unix time
		Timestamp: time.Unix(0, 0).Add(-1 * time.Millisecond),
	}
	response.ThrottleTime = 100 * time.Millisecond
	_, err := encode(&response, nil)
	if err == nil {
		t.Error("Expecting error, got nil")
	}
	target := PacketEncodingError{}
	if !errors.As(err, &target) {
		t.Error("Expecting PacketEncodingError, got:", err)
	}
}

//go:build !functional

package sarama

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	offsetFetchRequestNoGroupNoPartitions = []byte{
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}

	offsetFetchRequestNoPartitionsV6 = []byte{
		0x05, 'b', 'l', 'a', 'h', 0x01, 0x00,
	}

	offsetFetchRequestNoPartitionsV7 = []byte{
		0x05, 'b', 'l', 'a', 'h', 0x01, 0x01, 0x00,
	}

	offsetFetchRequestNoPartitions = []byte{
		0x00, 0x04, 'b', 'l', 'a', 'h',
		0x00, 0x00, 0x00, 0x00,
	}

	offsetFetchRequestOnePartition = []byte{
		0x00, 0x04, 'b', 'l', 'a', 'h',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x0D, 't', 'o', 'p', 'i', 'c', 'T', 'h', 'e', 'F', 'i', 'r', 's', 't',
		0x00, 0x00, 0x00, 0x01,
		0x4F, 0x4F, 0x4F, 0x4F,
	}

	offsetFetchRequestOnePartitionV6 = []byte{
		0x05, 'b', 'l', 'a', 'h',
		0x02, 0x0E, 't', 'o', 'p', 'i', 'c', 'T', 'h', 'e', 'F', 'i', 'r', 's', 't',
		0x02,
		0x4F, 0x4F, 0x4F, 0x4F,
		0x00, 0x00,
	}

	offsetFetchRequestOnePartitionV7 = []byte{
		0x05, 'b', 'l', 'a', 'h',
		0x02, 0x0E, 't', 'o', 'p', 'i', 'c', 'T', 'h', 'e', 'F', 'i', 'r', 's', 't',
		0x02,
		0x4F, 0x4F, 0x4F, 0x4F,
		0x00, 0x00, 0x00,
	}

	offsetFetchRequestNoPartitionsV8 = []byte{
		0x02,                     // Groups array, length 1
		0x05, 'b', 'l', 'a', 'h', // GroupId "blah"
		0x01, // Topics array, length 0
		0x00, // per-group tagged fields
		0x01, // RequireStable = true
		0x00, // top-level tagged fields
	}

	offsetFetchRequestOnePartitionV8 = []byte{
		0x02,                     // Groups array, length 1
		0x05, 'b', 'l', 'a', 'h', // GroupId "blah"
		0x02,                                                                  // Topics array, length 1
		0x0E, 't', 'o', 'p', 'i', 'c', 'T', 'h', 'e', 'F', 'i', 'r', 's', 't', // topic name
		0x02,                   // PartitionIndexes array, length 1
		0x4F, 0x4F, 0x4F, 0x4F, // partition index
		0x00, // per-topic tagged fields
		0x00, // per-group tagged fields
		0x00, // RequireStable = false
		0x00, // top-level tagged fields
	}

	offsetFetchRequestAllPartitions = []byte{
		0x00, 0x04, 'b', 'l', 'a', 'h',
		0xff, 0xff, 0xff, 0xff,
	}

	offsetFetchRequestAllPartitionsV8 = []byte{
		0x02,                     // Groups array, length 1
		0x05, 'b', 'l', 'a', 'h', // GroupId "blah"
		0x00, // Topics array, null (fetch all topics)
		0x00, // per-group tagged fields
		0x00, // RequireStable = false
		0x00, // top-level tagged fields
	}

	offsetFetchRequestTwoGroupsV8 = []byte{
		0x03,                     // Groups array, length 2
		0x05, 'b', 'l', 'a', 'h', // GroupId "blah"
		0x01,                // Topics array, length 0
		0x00,                // per-group tagged fields
		0x04, 'q', 'u', 'x', // GroupId "qux"
		0x01, // Topics array, length 0
		0x00, // per-group tagged fields
		0x00, // RequireStable = false
		0x00, // top-level tagged fields
	}
)

func TestOffsetFetchRequestNoPartitions(t *testing.T) {
	for version := 0; version <= 5; version++ {
		request := new(OffsetFetchRequest)
		request.Version = int16(version)
		request.ZeroPartitions()
		testRequest(t, fmt.Sprintf("no group, no partitions %d", version), request, offsetFetchRequestNoGroupNoPartitions)

		request.ConsumerGroup = "blah"
		testRequest(t, fmt.Sprintf("no partitions %d", version), request, offsetFetchRequestNoPartitions)
	}

	{ // v6
		version := 6
		request := new(OffsetFetchRequest)
		request.Version = int16(version)
		request.ConsumerGroup = "blah"
		request.ZeroPartitions()

		testRequest(t, fmt.Sprintf("no partitions %d", version), request, offsetFetchRequestNoPartitionsV6)
	}

	{ // v7
		version := 7
		request := new(OffsetFetchRequest)
		request.Version = int16(version)
		request.ConsumerGroup = "blah"
		request.RequireStable = true
		request.ZeroPartitions()

		testRequest(t, fmt.Sprintf("no partitions %d", version), request, offsetFetchRequestNoPartitionsV7)
	}

	{ // v8
		request := &OffsetFetchRequest{
			Version:       8,
			RequireStable: true,
			Groups: []OffsetFetchRequestGroup{
				{GroupId: "blah", Partitions: map[string][]int32{}},
			},
		}
		testRequest(t, "no partitions 8", request, offsetFetchRequestNoPartitionsV8)
	}
}

func TestOffsetFetchRequest(t *testing.T) {
	for version := 0; version <= 5; version++ {
		request := new(OffsetFetchRequest)
		request.Version = int16(version)
		request.ConsumerGroup = "blah"
		request.AddPartition("topicTheFirst", 0x4F4F4F4F)
		testRequest(t, fmt.Sprintf("one partition %d", version), request, offsetFetchRequestOnePartition)
	}

	{ // v6
		version := 6
		request := new(OffsetFetchRequest)
		request.Version = int16(version)
		request.ConsumerGroup = "blah"
		request.AddPartition("topicTheFirst", 0x4F4F4F4F)
		testRequest(t, fmt.Sprintf("one partition %d", version), request, offsetFetchRequestOnePartitionV6)
	}

	{ // v7
		version := 7
		request := new(OffsetFetchRequest)
		request.Version = int16(version)
		request.ConsumerGroup = "blah"
		request.AddPartition("topicTheFirst", 0x4F4F4F4F)
		testRequest(t, fmt.Sprintf("one partition %d", version), request, offsetFetchRequestOnePartitionV7)
	}

	{ // v8
		request := &OffsetFetchRequest{
			Version: 8,
			Groups: []OffsetFetchRequestGroup{
				{GroupId: "blah", Partitions: map[string][]int32{"topicTheFirst": {0x4F4F4F4F}}},
			},
		}
		testRequest(t, "one partition 8", request, offsetFetchRequestOnePartitionV8)
	}

	{ // v8, two groups
		request := &OffsetFetchRequest{
			Version: 8,
			Groups: []OffsetFetchRequestGroup{
				{GroupId: "blah", Partitions: map[string][]int32{}},
				{GroupId: "qux", Partitions: map[string][]int32{}},
			},
		}
		testRequest(t, "two groups v8", request, offsetFetchRequestTwoGroupsV8)
	}

	{ // downgraded single-group v8 request
		request := &OffsetFetchRequest{
			Version: 7,
			Groups: []OffsetFetchRequestGroup{
				{GroupId: "blah", Partitions: map[string][]int32{"topicTheFirst": {0x4F4F4F4F}}},
			},
		}
		testRequestEncode(t, "downgraded single group fallback", request, offsetFetchRequestOnePartitionV7)
	}
}

func TestOffsetFetchRequestValidation(t *testing.T) {
	t.Run("v8 with empty groups", func(t *testing.T) {
		request := &OffsetFetchRequest{Version: 8}
		_, err := encode(request, nil)
		require.ErrorContains(t, err, "version 8 or later requires Groups to be populated")
	})

	t.Run("pre-v8 with multiple groups", func(t *testing.T) {
		request := &OffsetFetchRequest{
			Version: 7,
			Groups: []OffsetFetchRequestGroup{
				{GroupId: "a"},
				{GroupId: "b"},
			},
		}
		_, err := encode(request, nil)
		require.ErrorContains(t, err, "multiple groups require version 8 or later")
	})
}

func TestOffsetFetchRequestAddGroupPartition(t *testing.T) {
	t.Run("creates a new group entry", func(t *testing.T) {
		request := &OffsetFetchRequest{Version: 8}
		request.AddGroupPartition("g1", "t1", 0)

		require.Len(t, request.Groups, 1)
		assert.Equal(t, "g1", request.Groups[0].GroupId)
		assert.Equal(t, []int32{0}, request.Groups[0].Partitions["t1"])
	})

	t.Run("appends to an existing group entry", func(t *testing.T) {
		request := &OffsetFetchRequest{
			Version: 8,
			Groups: []OffsetFetchRequestGroup{
				{GroupId: "g1", Partitions: map[string][]int32{"t1": {0}}},
			},
		}
		request.AddGroupPartition("g1", "t1", 1)
		request.AddGroupPartition("g1", "t2", 7)
		request.AddGroupPartition("g2", "t3", 2)

		require.Len(t, request.Groups, 2)
		assert.Equal(t, "g1", request.Groups[0].GroupId)
		assert.Equal(t, []int32{0, 1}, request.Groups[0].Partitions["t1"])
		assert.Equal(t, []int32{7}, request.Groups[0].Partitions["t2"])
		assert.Equal(t, "g2", request.Groups[1].GroupId)
		assert.Equal(t, []int32{2}, request.Groups[1].Partitions["t3"])
	})
}

func TestOffsetFetchRequestAllPartitions(t *testing.T) {
	for version := 2; version <= 5; version++ {
		request := &OffsetFetchRequest{Version: int16(version), ConsumerGroup: "blah"}
		testRequest(t, fmt.Sprintf("all partitions %d", version), request, offsetFetchRequestAllPartitions)
	}

	{ // v8
		request := &OffsetFetchRequest{
			Version: 8,
			Groups: []OffsetFetchRequestGroup{
				{GroupId: "blah", Partitions: nil},
			},
		}
		testRequest(t, "all partitions 8", request, offsetFetchRequestAllPartitionsV8)
	}

	{ // decoded v8 null topics array
		request := new(OffsetFetchRequest)
		require.NoError(t, versionedDecode(offsetFetchRequestAllPartitionsV8, request, 8, nil))
		require.Len(t, request.Groups, 1)
		assert.Nil(t, request.Groups[0].Partitions)

		packet := testRequestEncode(t, "decoded all partitions 8", request, offsetFetchRequestAllPartitionsV8)
		testRequestDecode(t, "decoded all partitions 8", request, packet)
	}
}

func TestNewOffsetFetchRequestDowngrade(t *testing.T) {
	request := NewOffsetFetchRequest(V3_0_0_0, "blah", map[string][]int32{"topicTheFirst": {0x4F4F4F4F}})
	request.setVersion(7)

	assert.Equal(t, "blah", request.ConsumerGroup)
	assert.Equal(t, map[string][]int32{"topicTheFirst": {int32(0x4F4F4F4F)}}, request.partitions)
	testRequestEncode(t, "downgraded single group v8 request", request, offsetFetchRequestOnePartitionV7)
}

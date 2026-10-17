//go:build !functional

package sarama

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func consumerGroupHeartbeatResponseFixture(version int16, tags []byte) []byte {
	b := []byte{
		0, 0, 0, 100, // throttle time
		0, 0, // no error
		0,          // null error message
		2, 'm',     // member ID
		0, 0, 0, 5, // member epoch
		0, 0, 0x0b, 0xb8, // heartbeat interval ms (3000)
		1,                                                     // assignment present
		2,                                                     // one topic partition
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, // UUID
		3, 0, 0, 0, 0, 0, 0, 0, 2, // partitions [0, 2]
	}
	b = append(b, tags...) // topic partitions tagged fields
	b = append(b, tags...) // assignment tagged fields
	return append(b, tags...) // response tagged fields
}

var (
	consumerGroupHeartbeatResponseV0 = consumerGroupHeartbeatResponseFixture(0, []byte{0})
	consumerGroupHeartbeatResponseV1 = consumerGroupHeartbeatResponseFixture(1, []byte{0})
)

func consumerGroupHeartbeatTestResponse(version int16) *ConsumerGroupHeartbeatResponse {
	return &ConsumerGroupHeartbeatResponse{
		Version:             version,
		ThrottleTime:        100 * time.Millisecond,
		ErrorCode:           ErrNoError,
		MemberId:            new("m"),
		MemberEpoch:         5,
		HeartbeatIntervalMs: 3000,
		Assignment: &ConsumerGroupHeartbeatAssignment{
			TopicPartitions: []ConsumerGroupHeartbeatTopicPartitions{{
				TopicID:    Uuid{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
				Partitions: []int32{0, 2},
			}},
		},
	}
}

func TestConsumerGroupHeartbeatResponse(t *testing.T) {
	for _, version := range []int16{0, 1} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			expected := consumerGroupHeartbeatTestResponse(version)
			fixture := consumerGroupHeartbeatResponseFixture(version, []byte{0})

			testResponse(t, "member and assignment", expected, fixture)
			require.Equal(t, 100*time.Millisecond, expected.throttleTime())
			require.Equal(t, int16(1), expected.headerVersion())
			require.Equal(t, NewConsumerGroupHeartbeatRequest(expected.requiredVersion()).Version, version)

			var decoded ConsumerGroupHeartbeatResponse
			withTags := consumerGroupHeartbeatResponseFixture(version, []byte{1, 42, 2, 0xab, 0xcd})
			require.NoError(t, versionedDecode(withTags, &decoded, version, nil))
			require.Equal(t, expected, &decoded)

			for n := range len(fixture) {
				require.Error(t, versionedDecode(fixture[:n], &ConsumerGroupHeartbeatResponse{}, version, nil), "truncated at %d", n)
			}
		})
	}
}

func TestConsumerGroupHeartbeatResponseNilAssignment(t *testing.T) {
	for _, version := range []int16{0, 1} {
		res := &ConsumerGroupHeartbeatResponse{
			Version:             version,
			ThrottleTime:        100 * time.Millisecond,
			ErrorCode:           ErrNoError,
			MemberId:            new("m"),
			MemberEpoch:         5,
			HeartbeatIntervalMs: 3000,
			Assignment:          nil,
		}
		fixture := []byte{
			0, 0, 0, 100, // throttle time
			0, 0, // no error
			0,          // null error message
			2, 'm',     // member ID
			0, 0, 0, 5, // member epoch
			0, 0, 0x0b, 0xb8, // heartbeat interval ms (3000)
			255, // null assignment (-1)
			0,   // response tagged fields
		}

		testResponse(t, "nil assignment", res, fixture)
	}
}

func TestConsumerGroupHeartbeatResponseError(t *testing.T) {
	for _, version := range []int16{0, 1} {
		res := &ConsumerGroupHeartbeatResponse{
			Version:             version,
			ThrottleTime:        0,
			ErrorCode:           ErrGroupIDNotFound,
			ErrorMessage:        new("missing"),
			MemberId:            nil,
			MemberEpoch:         0,
			HeartbeatIntervalMs: 0,
			Assignment:          nil,
		}
		fixture := []byte{
			0, 0, 0, 0, // throttle time
			0, 69, // GROUP_ID_NOT_FOUND (69)
			8, 'm', 'i', 's', 's', 'i', 'n', 'g', // error message
			0,          // null member ID
			0, 0, 0, 0, // member epoch
			0, 0, 0, 0, // heartbeat interval
			255, // null assignment
			0,   // tagged fields
		}

		testResponse(t, "group not found error", res, fixture)
	}
}

func TestConsumerGroupHeartbeatResponseEmptyAssignment(t *testing.T) {
	for _, version := range []int16{0, 1} {
		res := consumerGroupHeartbeatTestResponse(version)
		res.Assignment.TopicPartitions = []ConsumerGroupHeartbeatTopicPartitions{}
		fixture := []byte{
			0, 0, 0, 100, // throttle time
			0, 0, // no error
			0,          // null error message
			2, 'm',     // member ID
			0, 0, 0, 5, // member epoch
			0, 0, 0x0b, 0xb8, // heartbeat interval ms (3000)
			1, // assignment present
			1, // empty topic partitions array (0 elements = 1)
			0, // assignment tagged fields
			0, // response tagged fields
		}

		testResponse(t, "empty assignment", res, fixture)
	}
}

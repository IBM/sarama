//go:build !functional

package sarama

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These wire fixtures follow Apache Kafka's ConsumerGroupDescribe schema.
// Tagged fields occur at every struct boundary, including both assignments.
func consumerGroupDescribeResponseFixture(version int16, tags []byte) []byte {
	b := []byte{
		0, 0, 0, 100, // throttle time
		2,    // one group
		0, 0, // no error
		0, // null error message
		2, 'g',
		7, 'S', 't', 'a', 'b', 'l', 'e',
		0, 0, 0, 7, // group epoch
		0, 0, 0, 6, // assignment epoch
		8, 'u', 'n', 'i', 'f', 'o', 'r', 'm',
		2, // one member
		2, 'm',
		2, 'i', // instance ID
		1,          // empty (non-null) rack ID
		0, 0, 0, 5, // member epoch
		2, 'c',
		2, 'h',
		2, 2, 't', // subscribed topic names
		0,                                                     // null subscribed regex
		2,                                                     // current assignment: one topic
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, // UUID
		2, 't',
		3, 0, 0, 0, 0, 0, 0, 0, 2, // partitions 0, 2
	}
	b = append(b, tags...) // current topic
	b = append(b, tags...) // current assignment
	b = append(b,
		2, // target assignment: one topic
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		2, 't',
		2, 0, 0, 0, 2, // partition 2
	)
	b = append(b, tags...) // target topic
	b = append(b, tags...) // target assignment
	if version >= 1 {
		b = append(b, 1) // consumer member
	}
	b = append(b, tags...)     // member
	b = append(b, 0, 0, 0, 42) // authorized operations
	b = append(b, tags...)     // group
	return append(b, tags...)  // response
}

var (
	consumerGroupDescribeResponseV0 = consumerGroupDescribeResponseFixture(0, []byte{0})
	consumerGroupDescribeResponseV1 = consumerGroupDescribeResponseFixture(1, []byte{0})
)

func consumerGroupDescribeTestResponse(version int16) *ConsumerGroupDescribeResponse {
	memberType := int8(-1)
	if version >= 1 {
		memberType = 1
	}
	topicID := Uuid{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	return &ConsumerGroupDescribeResponse{
		Version:      version,
		ThrottleTime: 100 * time.Millisecond,
		Groups: []ConsumerGroupDescription{{
			GroupID: "g", GroupState: "Stable", GroupEpoch: 7,
			AssignmentEpoch: 6, AssignorName: "uniform", AuthorizedOperations: 42,
			Members: []ConsumerGroupMemberDescription{{
				MemberID: "m", InstanceID: nullString("i"), RackID: nullString(""),
				MemberEpoch: 5, ClientID: "c", ClientHost: "h",
				SubscribedTopicNames: []string{"t"}, MemberType: memberType,
				Assignment: ConsumerGroupAssignment{TopicPartitions: []ConsumerGroupTopicPartitions{{
					TopicID: topicID, TopicName: "t", Partitions: []int32{0, 2},
				}}},
				TargetAssignment: ConsumerGroupAssignment{TopicPartitions: []ConsumerGroupTopicPartitions{{
					TopicID: topicID, TopicName: "t", Partitions: []int32{2},
				}}},
			}},
		}},
	}
}

func TestConsumerGroupDescribeResponse(t *testing.T) {
	for _, version := range []int16{0, 1} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			expected := consumerGroupDescribeTestResponse(version)
			fixture := consumerGroupDescribeResponseFixture(version, []byte{0})
			testResponse(t, "member and assignments", expected, fixture)
			require.Equal(t, 100*time.Millisecond, expected.throttleTime())
			require.Equal(t, int16(1), expected.headerVersion())
			require.Equal(t, NewConsumerGroupDescribeRequest(expected.requiredVersion()).Version, version)

			var decoded ConsumerGroupDescribeResponse
			// Unknown tag 42 with a two-byte payload at every struct boundary.
			require.NoError(t, versionedDecode(consumerGroupDescribeResponseFixture(version, []byte{1, 42, 2, 0xab, 0xcd}), &decoded, version, nil))
			require.Equal(t, expected, &decoded)

			for n := range len(fixture) {
				require.Error(t, versionedDecode(fixture[:n], &ConsumerGroupDescribeResponse{}, version, nil), "truncated at %d", n)
			}
			testResponse(t, "empty groups", &ConsumerGroupDescribeResponse{
				Version: version, Groups: []ConsumerGroupDescription{},
			}, []byte{0, 0, 0, 0, 1, 0})
			require.Error(t, versionedDecode([]byte{0, 0, 0, 0, 255, 255, 255, 255, 15}, &ConsumerGroupDescribeResponse{}, version, nil), "oversized groups array")
		})
	}
}

func TestConsumerGroupDescribeResponseError(t *testing.T) {
	for _, version := range []int16{0, 1} {
		// A group error is part of a successfully decoded response.
		testResponse(t, "group not found", &ConsumerGroupDescribeResponse{
			Version: version,
			Groups: []ConsumerGroupDescription{{
				ErrorCode: ErrGroupIDNotFound, ErrorMessage: nullString("missing"),
				GroupID: "g", Members: []ConsumerGroupMemberDescription{},
				AuthorizedOperations: -2147483648,
			}},
		}, []byte{
			0, 0, 0, 0, 2, // throttle, one group
			0, 69, // GROUP_ID_NOT_FOUND
			8, 'm', 'i', 's', 's', 'i', 'n', 'g',
			2, 'g', 1, // group ID, empty state
			0, 0, 0, 0, 0, 0, 0, 0, // epochs
			1, 1, // empty assignor, empty members
			128, 0, 0, 0, // authorized operations not requested
			0, 0,
		})
	}
}

func TestConsumerGroupDescribeResponseEmptyAssignment(t *testing.T) {
	for _, version := range []int16{0, 1} {
		res := consumerGroupDescribeTestResponse(version)
		member := &res.Groups[0].Members[0]
		member.InstanceID = nil
		member.RackID = nil
		member.SubscribedTopicNames = nil
		member.SubscribedTopicRegex = nullString("t.*")
		member.Assignment.TopicPartitions = []ConsumerGroupTopicPartitions{}
		member.TargetAssignment.TopicPartitions[0].Partitions = []int32{}
		testResponse(t, "empty assignment and regex subscription", res, nil)
	}
}

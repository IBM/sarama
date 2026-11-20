//go:build !functional

package sarama

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func consumerGroupHeartbeatRequestFixture(version int16, tags []byte) []byte {
	b := []byte{
		2, 'g', // group ID
		2, 'm', // member ID
		0, 0, 0, 5, // member epoch
		2, 'i', // instance ID
		2, 'r', // rack ID
		0, 0, 0xaf, 0xc8, // rebalance timeout ms (45000)
		2, 2, 't', // subscribed topic names (one topic: "t")
	}

	if version >= 1 {
		b = append(b, 4, 't', '.', '*') // subscribed topic regex ("t.*")
	}

	b = append(b,
		8, 'u', 'n', 'i', 'f', 'o', 'r', 'm', // server assignor ("uniform")
		2, // one topic partition
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, // UUID
		3, 0, 0, 0, 0, 0, 0, 0, 2, // partitions [0, 2]
	)
	b = append(b, tags...) // topic partition tagged fields
	return append(b, tags...) // request tagged fields
}

var (
	consumerGroupHeartbeatRequestV0 = consumerGroupHeartbeatRequestFixture(0, []byte{0})
	consumerGroupHeartbeatRequestV1 = consumerGroupHeartbeatRequestFixture(1, []byte{0})
)

func consumerGroupHeartbeatTestRequest(version int16) *ConsumerGroupHeartbeatRequest {
	req := &ConsumerGroupHeartbeatRequest{
		Version:              version,
		GroupId:              "g",
		MemberId:             "m",
		MemberEpoch:          5,
		InstanceId:           new("i"),
		RackId:               new("r"),
		RebalanceTimeoutMs:   45000,
		SubscribedTopicNames: []string{"t"},
		ServerAssignor:       new("uniform"),
		TopicPartitions: []ConsumerGroupHeartbeatTopicPartitions{{
			TopicID:    Uuid{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			Partitions: []int32{0, 2},
		}},
	}

	if version >= 1 {
		req.SubscribedTopicRegex = new("t.*")
	}

	return req
}

func TestConsumerGroupHeartbeatRequest(t *testing.T) {
	for _, version := range []int16{0, 1} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			req := consumerGroupHeartbeatTestRequest(version)
			fixture := consumerGroupHeartbeatRequestFixture(version, []byte{0})

			testRequest(t, "full request", req, fixture)

			emptyReq := &ConsumerGroupHeartbeatRequest{
				Version:            version,
				RebalanceTimeoutMs: -1,
			}
			var emptyFixture []byte
			if version == 0 {
				emptyFixture = []byte{1, 1, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 0}
			} else {
				emptyFixture = []byte{1, 1, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 0, 0}
			}
			testRequest(t, "empty request", emptyReq, emptyFixture)

			withTags := consumerGroupHeartbeatRequestFixture(version, []byte{1, 42, 2, 0xab, 0xcd})
			var decoded ConsumerGroupHeartbeatRequest
			require.NoError(t, versionedDecode(withTags, &decoded, version, nil))
			require.Equal(t, req, &decoded)

			for n := range len(fixture) {
				require.Error(t, versionedDecode(fixture[:n], &ConsumerGroupHeartbeatRequest{}, version, nil), "truncated at %d", n)
			}
		})
	}
}

func TestNewConsumerGroupHeartbeatRequest(t *testing.T) {
	for _, tc := range []struct {
		kafka    KafkaVersion
		version  int16
		required KafkaVersion
	}{
		{V3_5_0_0, 0, V3_5_0_0},
		{V3_6_0_0, 0, V3_5_0_0},
		{V3_7_0_0, 0, V3_5_0_0},
		{V3_8_0_0, 0, V3_5_0_0},
		{V3_9_0_0, 0, V3_5_0_0},
		{V4_0_0_0, 1, V4_0_0_0},
		{MaxVersion, 1, V4_0_0_0},
	} {
		req := NewConsumerGroupHeartbeatRequest(tc.kafka)

		require.Equal(t, tc.version, req.Version)
		require.Equal(t, tc.required, req.requiredVersion())
		require.Equal(t, int16(68), req.key())
		require.Equal(t, int16(2), req.headerVersion())
		require.Equal(t, int32(-1), req.RebalanceTimeoutMs)
	}
}

func TestConsumerGroupHeartbeatInvalidVersion(t *testing.T) {
	for _, version := range []int16{-1, 2} {
		req := &ConsumerGroupHeartbeatRequest{Version: version}

		_, err := encode(req, nil)
		require.Error(t, err)
		require.Error(t, versionedDecode(consumerGroupHeartbeatRequestV0, req, version, nil))

		res := &ConsumerGroupHeartbeatResponse{Version: version}
		_, err = encode(res, nil)
		require.Error(t, err)
		require.Error(t, versionedDecode(consumerGroupHeartbeatResponseV0, res, version, nil))
	}
}

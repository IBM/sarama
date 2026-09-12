//go:build !functional

package sarama

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var consumerGroupDescribeRequestV0 = []byte{
	3, // two group IDs
	2, 'g',
	2, 'h',
	1, // include authorized operations
	0, // tagged fields
}

func TestConsumerGroupDescribeRequest(t *testing.T) {
	for _, version := range []int16{0, 1} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			req := &ConsumerGroupDescribeRequest{
				Version:                     version,
				GroupIDs:                    []string{"g", "h"},
				IncludeAuthorizedOperations: true,
			}
			testRequest(t, "groups", req, consumerGroupDescribeRequestV0)
			testRequest(t, "empty", &ConsumerGroupDescribeRequest{
				Version: version,
			}, []byte{1, 0, 0})

			withTags := append([]byte{}, consumerGroupDescribeRequestV0[:len(consumerGroupDescribeRequestV0)-1]...)
			withTags = append(withTags, 1, 42, 2, 0xab, 0xcd)
			var decoded ConsumerGroupDescribeRequest
			require.NoError(t, versionedDecode(withTags, &decoded, version, nil))
			require.Equal(t, req, &decoded)

			for n := range len(consumerGroupDescribeRequestV0) {
				require.Error(t, versionedDecode(consumerGroupDescribeRequestV0[:n], &ConsumerGroupDescribeRequest{}, version, nil), "truncated at %d", n)
			}
			require.Error(t, versionedDecode([]byte{255, 255, 255, 255, 15}, &ConsumerGroupDescribeRequest{}, version, nil), "oversized group IDs array")
		})
	}
}

func TestNewConsumerGroupDescribeRequest(t *testing.T) {
	for _, tc := range []struct {
		kafka    KafkaVersion
		version  int16
		required KafkaVersion
	}{
		{V3_7_0_0, 0, V3_7_0_0},
		{V3_8_0_0, 0, V3_7_0_0},
		{V3_9_0_0, 0, V3_7_0_0},
		{V4_0_0_0, 1, V4_0_0_0},
		{MaxVersion, 1, V4_0_0_0},
	} {
		req := NewConsumerGroupDescribeRequest(tc.kafka)
		require.Equal(t, tc.version, req.Version)
		require.Equal(t, tc.required, req.requiredVersion())
		require.Equal(t, int16(69), req.key())
		require.Equal(t, int16(2), req.headerVersion())
	}
}

func TestConsumerGroupDescribeInvalidVersion(t *testing.T) {
	for _, version := range []int16{-1, 2} {
		req := &ConsumerGroupDescribeRequest{Version: version}
		_, err := encode(req, nil)
		require.Error(t, err)
		require.Error(t, versionedDecode(consumerGroupDescribeRequestV0, req, version, nil))
		res := &ConsumerGroupDescribeResponse{Version: version}
		_, err = encode(res, nil)
		require.Error(t, err)
		require.Error(t, versionedDecode(consumerGroupDescribeResponseV0, res, version, nil))
	}
}

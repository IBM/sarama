//go:build !functional

package sarama

import (
	"fmt"
	"testing"
)

var describeQuorumRequestV0 = []byte{
	2,                                                                                            // Topics
	19, '_', '_', 'c', 'l', 'u', 's', 't', 'e', 'r', '_', 'm', 'e', 't', 'a', 'd', 'a', 't', 'a', // TopicName
	2,          // Partitions
	0, 0, 0, 0, // PartitionIndex
	0, // empty tagged fields
	0, // empty tagged fields
	0, // empty tagged fields
}

func TestDescribeQuorumRequest(t *testing.T) {
	// the request body is unchanged across versions
	for _, version := range []int16{0, 1, 2} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			request := &DescribeQuorumRequest{
				Version: version,
				Topics: []DescribeQuorumRequestTopic{
					{
						TopicName:        "__cluster_metadata",
						PartitionIndexes: []int32{0},
					},
				},
			}

			testRequest(t, "DescribeQuorumRequest", request, describeQuorumRequestV0)
		})
	}
}

func TestNewDescribeQuorumRequest(t *testing.T) {
	for _, tt := range []struct {
		kafkaVersion KafkaVersion
		version      int16
	}{
		{V2_7_0_0, 0},
		{V3_2_0_0, 0},
		{V3_3_0_0, 1},
		{V3_8_0_0, 1},
		{V3_9_0_0, 2},
		{V4_0_0_0, 2},
	} {
		if v := NewDescribeQuorumRequest(tt.kafkaVersion).Version; v != tt.version {
			t.Errorf("NewDescribeQuorumRequest(%s) version = %d, want %d", tt.kafkaVersion, v, tt.version)
		}
	}
}

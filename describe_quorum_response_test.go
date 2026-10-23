//go:build !functional

package sarama

import "testing"

var (
	describeQuorumResponseV0 = []byte{
		0, 0, // ErrorCode
		2,                                                                                            // Topics
		19, '_', '_', 'c', 'l', 'u', 's', 't', 'e', 'r', '_', 'm', 'e', 't', 'a', 'd', 'a', 't', 'a', // TopicName
		2,          // Partitions
		0, 0, 0, 0, // PartitionIndex
		0, 0, // ErrorCode
		0, 0, 0, 1, // LeaderId
		0, 0, 0, 5, // LeaderEpoch
		0, 0, 0, 0, 0, 0, 0, 100, // HighWatermark
		2,          // CurrentVoters
		0, 0, 0, 1, // ReplicaId
		0, 0, 0, 0, 0, 0, 0, 100, // LogEndOffset
		0, // empty tagged fields
		1, // Observers
		0, // empty tagged fields
		0, // empty tagged fields
		0, // empty tagged fields
	}

	describeQuorumResponseV1 = []byte{
		0, 0, // ErrorCode
		2,                                                                                            // Topics
		19, '_', '_', 'c', 'l', 'u', 's', 't', 'e', 'r', '_', 'm', 'e', 't', 'a', 'd', 'a', 't', 'a', // TopicName
		2,          // Partitions
		0, 0, 0, 0, // PartitionIndex
		0, 0, // ErrorCode
		0, 0, 0, 1, // LeaderId
		0, 0, 0, 5, // LeaderEpoch
		0, 0, 0, 0, 0, 0, 0, 100, // HighWatermark
		2,          // CurrentVoters
		0, 0, 0, 1, // ReplicaId
		0, 0, 0, 0, 0, 0, 0, 100, // LogEndOffset
		255, 255, 255, 255, 255, 255, 255, 255, // LastFetchTimestamp
		0, 0, 0, 0, 0, 0, 3, 232, // LastCaughtUpTimestamp
		0,          // empty tagged fields
		2,          // Observers
		0, 0, 0, 4, // ReplicaId
		0, 0, 0, 0, 0, 0, 0, 99, // LogEndOffset
		0, 0, 0, 0, 0, 0, 3, 231, // LastFetchTimestamp
		0, 0, 0, 0, 0, 0, 3, 230, // LastCaughtUpTimestamp
		0, // empty tagged fields
		0, // empty tagged fields
		0, // empty tagged fields
		0, // empty tagged fields
	}

	describeQuorumResponseV2 = []byte{
		0, 0, // ErrorCode
		0,                                                                                            // ErrorMessage
		2,                                                                                            // Topics
		19, '_', '_', 'c', 'l', 'u', 's', 't', 'e', 'r', '_', 'm', 'e', 't', 'a', 'd', 'a', 't', 'a', // TopicName
		2,          // Partitions
		0, 0, 0, 0, // PartitionIndex
		0, 0, // ErrorCode
		0,          // ErrorMessage
		0, 0, 0, 1, // LeaderId
		0, 0, 0, 5, // LeaderEpoch
		0, 0, 0, 0, 0, 0, 0, 100, // HighWatermark
		2,          // CurrentVoters
		0, 0, 0, 1, // ReplicaId
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, // ReplicaDirectoryId
		0, 0, 0, 0, 0, 0, 0, 100, // LogEndOffset
		255, 255, 255, 255, 255, 255, 255, 255, // LastFetchTimestamp
		0, 0, 0, 0, 0, 0, 3, 232, // LastCaughtUpTimestamp
		0,          // empty tagged fields
		1,          // Observers
		0,          // empty tagged fields
		0,          // empty tagged fields
		2,          // Nodes
		0, 0, 0, 1, // NodeId
		2,                                                    // Listeners
		11, 'C', 'O', 'N', 'T', 'R', 'O', 'L', 'L', 'E', 'R', // Name
		10, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't', // Host
		156, 64, // Port
		0, // empty tagged fields
		0, // empty tagged fields
		0, // empty tagged fields
	}
)

func TestDescribeQuorumResponse(t *testing.T) {
	t.Run("v0", func(t *testing.T) {
		response := &DescribeQuorumResponse{
			Version:   0,
			ErrorCode: ErrNoError,
			Topics: []DescribeQuorumResponseTopic{
				{
					TopicName: "__cluster_metadata",
					Partitions: []DescribeQuorumResponsePartition{
						{
							PartitionIndex: 0,
							ErrorCode:      ErrNoError,
							LeaderID:       1,
							LeaderEpoch:    5,
							HighWatermark:  100,
							CurrentVoters: []QuorumReplicaState{
								// v0 has no timestamps, which decode as unknown
								{ReplicaID: 1, LogEndOffset: 100, LastFetchTimestamp: -1, LastCaughtUpTimestamp: -1},
							},
							Observers: []QuorumReplicaState{},
						},
					},
				},
			},
		}

		testResponse(t, "v0", response, describeQuorumResponseV0)
	})

	t.Run("v1", func(t *testing.T) {
		response := &DescribeQuorumResponse{
			Version:   1,
			ErrorCode: ErrNoError,
			Topics: []DescribeQuorumResponseTopic{
				{
					TopicName: "__cluster_metadata",
					Partitions: []DescribeQuorumResponsePartition{
						{
							PartitionIndex: 0,
							ErrorCode:      ErrNoError,
							LeaderID:       1,
							LeaderEpoch:    5,
							HighWatermark:  100,
							CurrentVoters: []QuorumReplicaState{
								{ReplicaID: 1, LogEndOffset: 100, LastFetchTimestamp: -1, LastCaughtUpTimestamp: 1000},
							},
							Observers: []QuorumReplicaState{
								{ReplicaID: 4, LogEndOffset: 99, LastFetchTimestamp: 999, LastCaughtUpTimestamp: 998},
							},
						},
					},
				},
			},
		}

		testResponse(t, "v1", response, describeQuorumResponseV1)
	})

	t.Run("v2", func(t *testing.T) {
		response := &DescribeQuorumResponse{
			Version:   2,
			ErrorCode: ErrNoError,
			Topics: []DescribeQuorumResponseTopic{
				{
					TopicName: "__cluster_metadata",
					Partitions: []DescribeQuorumResponsePartition{
						{
							PartitionIndex: 0,
							ErrorCode:      ErrNoError,
							LeaderID:       1,
							LeaderEpoch:    5,
							HighWatermark:  100,
							CurrentVoters: []QuorumReplicaState{
								{
									ReplicaID:             1,
									ReplicaDirectoryID:    Uuid{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
									LogEndOffset:          100,
									LastFetchTimestamp:    -1,
									LastCaughtUpTimestamp: 1000,
								},
							},
							Observers: []QuorumReplicaState{},
						},
					},
				},
			},
			Nodes: []DescribeQuorumResponseNode{
				{
					NodeID: 1,
					Listeners: []DescribeQuorumResponseListener{
						{Name: "CONTROLLER", Host: "localhost", Port: 40000},
					},
				},
			},
		}

		testResponse(t, "v2", response, describeQuorumResponseV2)
	})
}

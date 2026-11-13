//go:build !functional

package sarama

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterAdminDescribeMetadataQuorum(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	quorum := NewMockDescribeQuorumResponse(t)
	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"DescribeQuorumRequest": quorum,
	})

	config := NewTestConfig()
	config.Version = V3_9_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	quorumAdmin := admin.(MetadataQuorumClusterAdmin)

	t.Run("returns the quorum state", func(t *testing.T) {
		voters := []QuorumReplicaState{{ReplicaID: 3, LogEndOffset: 100, LastFetchTimestamp: -1, LastCaughtUpTimestamp: 10}}
		observers := []QuorumReplicaState{{ReplicaID: 4, LogEndOffset: 99, LastFetchTimestamp: 9, LastCaughtUpTimestamp: 8}}
		nodes := []DescribeQuorumResponseNode{{NodeID: 3, Listeners: []DescribeQuorumResponseListener{{Name: "CONTROLLER", Host: "kafka-3", Port: 9093}}}}
		quorum.SetError(ErrNoError).SetQuorum(DescribeQuorumResponsePartition{
			LeaderID:      3,
			LeaderEpoch:   7,
			HighWatermark: 100,
			CurrentVoters: voters,
			Observers:     observers,
		}).SetNodes(nodes...)

		info, err := quorumAdmin.DescribeMetadataQuorum()
		require.NoError(t, err)
		require.Equal(t, &QuorumInfo{
			LeaderID:      3,
			LeaderEpoch:   7,
			HighWatermark: 100,
			Voters:        voters,
			Observers:     observers,
			Nodes:         nodes,
		}, info)

		var request *DescribeQuorumRequest
		for _, exchange := range seedBroker.History() {
			if req, ok := exchange.Request.(*DescribeQuorumRequest); ok {
				request = req
			}
		}
		require.NotNil(t, request)
		require.Equal(t, []DescribeQuorumRequestTopic{{TopicName: "__cluster_metadata", PartitionIndexes: []int32{0}}}, request.Topics)
	})

	t.Run("returns a top level error", func(t *testing.T) {
		quorum.SetError(ErrClusterAuthorizationFailed)

		_, err := quorumAdmin.DescribeMetadataQuorum()
		require.ErrorIs(t, err, ErrClusterAuthorizationFailed)
	})

	t.Run("returns a partition error", func(t *testing.T) {
		quorum.SetError(ErrNoError).SetQuorum(DescribeQuorumResponsePartition{ErrorCode: ErrNotLeaderForPartition})

		_, err := quorumAdmin.DescribeMetadataQuorum()
		require.ErrorIs(t, err, ErrNotLeaderForPartition)
	})
}

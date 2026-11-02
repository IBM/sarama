package sarama

import (
	"errors"
	"fmt"
)

// clusterMetadataTopic is the internal topic that holds the KRaft metadata
// log. Its single partition is replicated by the controller quorum.
const clusterMetadataTopic = "__cluster_metadata"

// MetadataQuorumClusterAdmin extends ClusterAdmin with the KIP-836
// DescribeMetadataQuorum API. Values returned by NewClusterAdmin and
// NewClusterAdminFromClient implement MetadataQuorumClusterAdmin.
type MetadataQuorumClusterAdmin interface {
	ClusterAdmin

	// DescribeMetadataQuorum describes the KRaft controller quorum, including
	// the id of the active controller. Requires a KRaft cluster running Kafka
	// 3.3.0.0 or higher.
	DescribeMetadataQuorum() (*QuorumInfo, error)
}

var _ MetadataQuorumClusterAdmin = (*clusterAdmin)(nil)

// QuorumInfo describes the state of the KRaft metadata quorum.
type QuorumInfo struct {
	// LeaderID is the id of the active controller, or -1 if it is unknown
	LeaderID int32

	// LeaderEpoch is the latest known leader epoch
	LeaderEpoch int32

	// HighWatermark is the high watermark of the metadata log
	HighWatermark int64

	// Voters are the controllers that take part in leader election
	Voters []QuorumReplicaState

	// Observers are the replicas that only follow the metadata log, such as
	// brokers
	Observers []QuorumReplicaState

	// Nodes contains the listeners of each voter. Requires Kafka 3.9.0.0 or
	// higher.
	Nodes []DescribeQuorumResponseNode
}

func (ca *clusterAdmin) DescribeMetadataQuorum() (*QuorumInfo, error) {
	request := NewDescribeQuorumRequest(ca.conf.Version)
	request.Topics = []DescribeQuorumRequestTopic{
		{TopicName: clusterMetadataTopic, PartitionIndexes: []int32{0}},
	}

	var info *QuorumInfo
	err := ca.retryOnControllerError(func() error {
		// any broker forwards the request to the active controller
		b, err := ca.Controller()
		if err != nil {
			return err
		}

		rsp, err := b.DescribeQuorum(request)
		if err != nil {
			return err
		}

		if !errors.Is(rsp.ErrorCode, ErrNoError) {
			if rsp.ErrorMessage != nil && *rsp.ErrorMessage != "" {
				return fmt.Errorf("%w: %s", rsp.ErrorCode, *rsp.ErrorMessage)
			}
			return rsp.ErrorCode
		}

		partition := rsp.metadataPartition()
		if partition == nil {
			return ErrIncompleteResponse
		}
		if !errors.Is(partition.ErrorCode, ErrNoError) {
			if partition.ErrorMessage != nil && *partition.ErrorMessage != "" {
				return fmt.Errorf("%w: %s", partition.ErrorCode, *partition.ErrorMessage)
			}
			return partition.ErrorCode
		}

		info = &QuorumInfo{
			LeaderID:      partition.LeaderID,
			LeaderEpoch:   partition.LeaderEpoch,
			HighWatermark: partition.HighWatermark,
			Voters:        partition.CurrentVoters,
			Observers:     partition.Observers,
			Nodes:         rsp.Nodes,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return info, nil
}

// metadataPartition returns the result for partition 0 of the
// __cluster_metadata topic, or nil if the response does not contain it
func (r *DescribeQuorumResponse) metadataPartition() *DescribeQuorumResponsePartition {
	for i := range r.Topics {
		if r.Topics[i].TopicName != clusterMetadataTopic {
			continue
		}
		for j := range r.Topics[i].Partitions {
			if r.Topics[i].Partitions[j].PartitionIndex == 0 {
				return &r.Topics[i].Partitions[j]
			}
		}
	}
	return nil
}

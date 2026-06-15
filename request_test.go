//go:build !functional

package sarama

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	assert "github.com/stretchr/testify/require"
)

//nolint:gosec // G101: false positive — values are Kafka API request type names, not credentials
var names = map[int16]string{
	apiKeyProduce:                      "ProduceRequest",
	apiKeyFetch:                        "FetchRequest",
	apiKeyListOffsets:                  "ListOffsetsRequest",
	apiKeyMetadata:                     "MetadataRequest",
	apiKeyLeaderAndIsr:                 "LeaderAndIsrRequest",
	apiKeyStopReplica:                  "StopReplicaRequest",
	apiKeyUpdateMetadata:               "UpdateMetadataRequest",
	apiKeyControlledShutdown:           "ControlledShutdownRequest",
	apiKeyOffsetCommit:                 "OffsetCommitRequest",
	apiKeyOffsetFetch:                  "OffsetFetchRequest",
	apiKeyFindCoordinator:              "FindCoordinatorRequest",
	apiKeyJoinGroup:                    "JoinGroupRequest",
	apiKeyHeartbeat:                    "HeartbeatRequest",
	apiKeyLeaveGroup:                   "LeaveGroupRequest",
	apiKeySyncGroup:                    "SyncGroupRequest",
	apiKeyDescribeGroups:               "DescribeGroupsRequest",
	apiKeyListGroups:                   "ListGroupsRequest",
	apiKeySaslHandshake:                "SaslHandshakeRequest",
	apiKeyApiVersions:                  "ApiVersionsRequest",
	apiKeyCreateTopics:                 "CreateTopicsRequest",
	apiKeyDeleteTopics:                 "DeleteTopicsRequest",
	apiKeyDeleteRecords:                "DeleteRecordsRequest",
	apiKeyInitProducerId:               "InitProducerIdRequest",
	apiKeyOffsetForLeaderEpoch:         "OffsetForLeaderEpochRequest",
	apiKeyAddPartitionsToTxn:           "AddPartitionsToTxnRequest",
	apiKeyAddOffsetsToTxn:              "AddOffsetsToTxnRequest",
	apiKeyEndTxn:                       "EndTxnRequest",
	apiKeyWriteTxnMarkers:              "WriteTxnMarkersRequest",
	apiKeyTxnOffsetCommit:              "TxnOffsetCommitRequest",
	apiKeyDescribeAcls:                 "DescribeAclsRequest",
	apiKeyCreateAcls:                   "CreateAclsRequest",
	apiKeyDeleteAcls:                   "DeleteAclsRequest",
	apiKeyDescribeConfigs:              "DescribeConfigsRequest",
	apiKeyAlterConfigs:                 "AlterConfigsRequest",
	apiKeyAlterReplicaLogDirs:          "AlterReplicaLogDirsRequest",
	apiKeyDescribeLogDirs:              "DescribeLogDirsRequest",
	apiKeySASLAuth:                     "SaslAuthenticateRequest",
	apiKeyCreatePartitions:             "CreatePartitionsRequest",
	apiKeyCreateDelegationToken:        "CreateDelegationTokenRequest",
	apiKeyRenewDelegationToken:         "RenewDelegationTokenRequest",
	apiKeyExpireDelegationToken:        "ExpireDelegationTokenRequest",
	apiKeyDescribeDelegationToken:      "DescribeDelegationTokenRequest",
	apiKeyDeleteGroups:                 "DeleteGroupsRequest",
	apiKeyElectLeaders:                 "ElectLeadersRequest",
	apiKeyIncrementalAlterConfigs:      "IncrementalAlterConfigsRequest",
	apiKeyAlterPartitionReassignments:  "AlterPartitionReassignmentsRequest",
	apiKeyListPartitionReassignments:   "ListPartitionReassignmentsRequest",
	apiKeyOffsetDelete:                 "OffsetDeleteRequest",
	apiKeyDescribeClientQuotas:         "DescribeClientQuotasRequest",
	apiKeyAlterClientQuotas:            "AlterClientQuotasRequest",
	apiKeyDescribeUserScramCredentials: "DescribeUserScramCredentialsRequest",
	apiKeyAlterUserScramCredentials:    "AlterUserScramCredentialsRequest",
	52:                                 "VoteRequest",
	53:                                 "BeginQuorumEpochRequest",
	54:                                 "EndQuorumEpochRequest",
	55:                                 "DescribeQuorumRequest",
	56:                                 "AlterPartitionRequest",
	apiKeyUpdateFeatures:               "UpdateFeaturesRequest",
	58:                                 "EnvelopeRequest",
	59:                                 "FetchSnapshotRequest",
	apiKeyDescribeCluster:              "DescribeClusterRequest",
	61:                                 "DescribeProducersRequest",
	62:                                 "BrokerRegistrationRequest",
	63:                                 "BrokerHeartbeatRequest",
	64:                                 "UnregisterBrokerRequest",
	65:                                 "DescribeTransactionsRequest",
	66:                                 "ListTransactionsRequest",
	67:                                 "AllocateProducerIdsRequest",
	68:                                 "ConsumerGroupHeartbeatRequest",
}

// TestAllocateBodyProtocolVersions tests two related version expectations:
//  1. uncommented entries are protocol versions Sarama currently supports with
//     parity to a given Kafka release version
//  2. commented TODO entries are the max versions supported by the given Kafka
//     release that Sarama still needs to implement for parity, this is for dev
//     tracking.
func TestAllocateBodyProtocolVersions(t *testing.T) {
	type test struct {
		version     KafkaVersion
		apiVersions map[int16]int16
	}

	saramaMaxVersions := newKafkaVersion(999, 999, 999, 999)
	maxVersion := func(pb protocolBody) int16 {
		var (
			i int16
			v int16
		)
		for i = 0; ; i++ {
			pb.setVersion(i)
			if !pb.isValidVersion() {
				return v
			}
			v = i
		}
	}

	tests := []test{
		{
			V1_1_0_0,
			map[int16]int16{
				apiKeyProduce:                 5,
				apiKeyFetch:                   7,
				apiKeyListOffsets:             2,
				apiKeyMetadata:                5,
				apiKeyLeaderAndIsr:            1,
				apiKeyStopReplica:             0,
				apiKeyUpdateMetadata:          4,
				apiKeyControlledShutdown:      1,
				apiKeyOffsetCommit:            3,
				apiKeyOffsetFetch:             3,
				apiKeyFindCoordinator:         1,
				apiKeyJoinGroup:               2,
				apiKeyHeartbeat:               1,
				apiKeyLeaveGroup:              1,
				apiKeySyncGroup:               1,
				apiKeyDescribeGroups:          1,
				apiKeyListGroups:              1,
				apiKeySaslHandshake:           1,
				apiKeyApiVersions:             1,
				apiKeyCreateTopics:            2,
				apiKeyDeleteTopics:            1,
				apiKeyDeleteRecords:           0,
				apiKeyInitProducerId:          0,
				apiKeyOffsetForLeaderEpoch:    0,
				apiKeyAddPartitionsToTxn:      0,
				apiKeyAddOffsetsToTxn:         0,
				apiKeyEndTxn:                  0,
				apiKeyWriteTxnMarkers:         0,
				apiKeyTxnOffsetCommit:         0,
				apiKeyDescribeAcls:            0,
				apiKeyCreateAcls:              0,
				apiKeyDeleteAcls:              0,
				apiKeyDescribeConfigs:         1,
				apiKeyAlterConfigs:            0,
				apiKeyAlterReplicaLogDirs:     0,
				apiKeyDescribeLogDirs:         0,
				apiKeySASLAuth:                0,
				apiKeyCreatePartitions:        0,
				apiKeyCreateDelegationToken:   0,
				apiKeyRenewDelegationToken:    0,
				apiKeyExpireDelegationToken:   0,
				apiKeyDescribeDelegationToken: 0,
				apiKeyDeleteGroups:            0,
			},
		},
		{
			V2_0_0_0,
			map[int16]int16{
				apiKeyProduce:                 6, // up from 5
				apiKeyFetch:                   8, // up from 7
				apiKeyListOffsets:             3, // up from 2
				apiKeyMetadata:                6, // up from 5
				apiKeyOffsetCommit:            4, // up from 3
				apiKeyOffsetFetch:             4, // up from 3
				apiKeyFindCoordinator:         2, // up from 1
				apiKeyJoinGroup:               3, // up from 2
				apiKeyHeartbeat:               2, // up from 1
				apiKeyLeaveGroup:              2, // up from 1
				apiKeySyncGroup:               2, // up from 1
				apiKeyDescribeGroups:          2, // up from 1
				apiKeyListGroups:              2, // up from 1
				apiKeyApiVersions:             2, // up from 1
				apiKeyCreateTopics:            3, // up from 2
				apiKeyDeleteTopics:            2, // up from 1
				apiKeyDeleteRecords:           1, // up from 0
				apiKeyInitProducerId:          1, // up from 0
				apiKeyOffsetForLeaderEpoch:    1, // up from 0
				apiKeyAddPartitionsToTxn:      1, // up from 0
				apiKeyAddOffsetsToTxn:         1, // up from 0
				apiKeyEndTxn:                  1, // up from 0
				apiKeyTxnOffsetCommit:         1, // up from 0
				apiKeyDescribeAcls:            1, // up from 0
				apiKeyCreateAcls:              1, // up from 0
				apiKeyDeleteAcls:              1, // up from 0
				apiKeyDescribeConfigs:         2, // up from 1
				apiKeyAlterConfigs:            1, // up from 0
				apiKeyAlterReplicaLogDirs:     1, // up from 0
				apiKeyDescribeLogDirs:         1, // up from 0
				apiKeyCreatePartitions:        1, // up from 0
				apiKeyCreateDelegationToken:   1, // up from 0
				apiKeyRenewDelegationToken:    1, // up from 0
				apiKeyExpireDelegationToken:   1, // up from 0
				apiKeyDescribeDelegationToken: 1, // up from 0
				apiKeyDeleteGroups:            1, // up from 0
			},
		},
		{
			V2_1_0_0,
			map[int16]int16{
				apiKeyProduce:              7,  // up from 6
				apiKeyFetch:                10, // up from 8
				apiKeyListOffsets:          4,  // up from 3
				apiKeyMetadata:             7,  // up from 6
				apiKeyOffsetCommit:         6,  // up from 4
				apiKeyOffsetFetch:          5,  // up from 4
				apiKeyDeleteTopics:         3,  // up from 2
				apiKeyOffsetForLeaderEpoch: 2,  // up from 1
				apiKeyTxnOffsetCommit:      2,  // up from 1
			},
		},
		{
			V2_2_0_0,
			map[int16]int16{
				apiKeyListOffsets:        5, // up from 4
				apiKeyLeaderAndIsr:       2, // up from 1
				apiKeyStopReplica:        1, // up from 0
				apiKeyUpdateMetadata:     5, // up from 4
				apiKeyControlledShutdown: 2, // up from 1
				apiKeyJoinGroup:          4, // up from 3
				apiKeySASLAuth:           1, // up from 0
				apiKeyElectLeaders:       0, // new in 2.2
			},
		},
		{
			V2_3_0_0,
			map[int16]int16{
				apiKeyFetch:                   11, // up from 10
				apiKeyMetadata:                8,  // up from 7
				apiKeyOffsetCommit:            7,  // up from 6
				apiKeyJoinGroup:               5,  // up from 4
				apiKeyHeartbeat:               3,  // up from 2
				apiKeySyncGroup:               3,  // up from 2
				apiKeyDescribeGroups:          3,  // up from 2
				apiKeyIncrementalAlterConfigs: 0,  // new in 2.3
			},
		},
		{
			V2_4_0_0,
			map[int16]int16{
				apiKeyProduce:                     8, // up from 7
				apiKeyMetadata:                    9, // up from 8
				apiKeyLeaderAndIsr:                4, // up from 2
				apiKeyStopReplica:                 2, // up from 1
				apiKeyUpdateMetadata:              6, // up from 5
				apiKeyControlledShutdown:          3, // up from 2
				apiKeyOffsetCommit:                8, // up from 7
				apiKeyOffsetFetch:                 6, // up from 5
				apiKeyFindCoordinator:             3, // up from 2
				apiKeyJoinGroup:                   6, // up from 5
				apiKeyHeartbeat:                   4, // up from 3
				apiKeyLeaveGroup:                  4, // up from 2
				apiKeySyncGroup:                   4, // up from 3
				apiKeyDescribeGroups:              5, // up from 3
				apiKeyListGroups:                  3, // up from 2
				apiKeyApiVersions:                 3, // up from 2
				apiKeyCreateTopics:                5, // up from 3
				apiKeyDeleteTopics:                4, // up from 3
				apiKeyInitProducerId:              2, // up from 1
				apiKeyDeleteGroups:                2, // up from 1
				apiKeyElectLeaders:                2, // up from 0
				apiKeyIncrementalAlterConfigs:     1, // up from 0
				apiKeyAlterPartitionReassignments: 0, // new in 2.4
				apiKeyListPartitionReassignments:  0, // new in 2.4
				apiKeyOffsetDelete:                0, // new in 2.4
			},
		},
		{
			V2_5_0_0,
			map[int16]int16{
				apiKeyOffsetFetch:      7, // up from 6
				apiKeyJoinGroup:        7, // up from 6
				apiKeyInitProducerId:   3, // up from 2
				apiKeyDescribeAcls:     2, // up from 1
				apiKeyCreateAcls:       2, // up from 1
				apiKeyDeleteAcls:       2, // up from 1
				apiKeySASLAuth:         2, // up from 1
				apiKeyCreatePartitions: 2, // up from 1
				apiKeySyncGroup:        5, // up from 4
				apiKeyTxnOffsetCommit:  3, // up from 2
			},
		},
		{
			V2_6_0_0,
			map[int16]int16{
				apiKeyListGroups:           4, // up from 3
				apiKeyDescribeLogDirs:      2, // up from 1
				apiKeyDescribeClientQuotas: 0, // new in 2.6
				apiKeyAlterClientQuotas:    0, // new in 2.6
				apiKeyDeleteRecords:        2, // up from 1
				apiKeyDescribeConfigs:      3, // up from 2
			},
		},
		{
			V2_7_0_0,
			map[int16]int16{
				apiKeyInitProducerId:               4,  // up from 3
				apiKeyAddPartitionsToTxn:           2,  // up from 1
				apiKeyAddOffsetsToTxn:              2,  // up from 1
				apiKeyEndTxn:                       2,  // up from 1
				apiKeyDescribeUserScramCredentials: 0,  // new in 2.7
				apiKeyAlterUserScramCredentials:    0,  // new in 2.7
				apiKeyFetch:                        12, // up from 11
				apiKeyCreateTopics:                 6,  // up from 5
				apiKeyDeleteTopics:                 5,  // up from 4
				apiKeyCreatePartitions:             3,  // up from 2
				apiKeyUpdateFeatures:               0,  // new in 2.7
			},
		},
		{
			V2_8_0_0,
			map[int16]int16{
				apiKeyMetadata:             11, // up from 9
				apiKeyDescribeClientQuotas: 1,  // up from 0
				apiKeyDescribeCluster:      0,  // new in 2.8
				apiKeyDescribeConfigs:      4,  // up from 3
				apiKeyAddPartitionsToTxn:   3,  // up from 2
				apiKeyProduce:              9,  // up from 8
				// TODO: ListOffsetsRequest v6 is not supported, but expected for KafkaVersion 2.8.0
				// apiKeyListOffsets:          6, // up from 5
				// TODO: AddOffsetsToTxnRequest v3 is not supported, but expected for KafkaVersion 2.8.0
				// apiKeyAddOffsetsToTxn:      3, // up from 2
				// TODO: EndTxnRequest v3 is not supported, but expected for KafkaVersion 2.8.0
				// apiKeyEndTxn:               3, // up from 2
				// TODO: AlterConfigsRequest v2 is not supported, but expected for KafkaVersion 2.8.0
				// apiKeyAlterConfigs:         2, // up from 1
				// TODO: AlterClientQuotasRequest v1 is not supported, but expected for KafkaVersion 2.8.0
				// apiKeyAlterClientQuotas:    1, // up from 0
				// TODO: CreateTopicsRequest v7 is not supported, but expected for KafkaVersion 2.8.0
				// apiKeyCreateTopics:         7, // up from 6
				// TODO: DeleteTopicsRequest v6 is not supported, but expected for KafkaVersion 2.8.0
				// apiKeyDeleteTopics:         6, // up from 5
				// TODO: DescribeProducersRequest v0 is not supported, but expected for KafkaVersion 2.8.0
				// apiKeyDescribeProducers /* (61) */: 0, // new in 2.8
			},
		},
		{
			V3_0_0_0,
			map[int16]int16{
				apiKeyOffsetFetch: 8, // up from 7
				// TODO: ListOffsetsRequest v7 is not supported, but expected for KafkaVersion 3.0.0
				// apiKeyListOffsets:     7, // up from 6
				// TODO: FindCoordinatorRequest v4 is not supported, but expected for KafkaVersion 3.0.0
				// apiKeyFindCoordinator: 4, // up from 3
			},
		},
		{
			V3_1_0_0,
			map[int16]int16{
				// TODO: FetchRequest v13 is not supported, but expected for KafkaVersion 3.1.0
				// apiKeyFetch:    13, // up from 12
				// TODO: MetadataRequest v12 is not supported, but expected for KafkaVersion 3.1.0
				// apiKeyMetadata: 12, // up from 11
			},
		},
		{
			V3_2_0_0,
			map[int16]int16{
				apiKeyJoinGroup:       8, // up from 7
				apiKeyLeaveGroup:      5, // up from 4
				apiKeyDescribeLogDirs: 3, // up from 2
				// TODO: JoinGroupRequest v9 is not supported, but expected for KafkaVersion 3.2.0
				// apiKeyJoinGroup:    9, // up from 7
			},
		},
		{
			V3_3_0_0,
			map[int16]int16{
				apiKeyDescribeLogDirs: 4, // up from 3
				// TODO: DescribeAclsRequest v3 is not supported, but expected for KafkaVersion 3.3.0
				// apiKeyDescribeAcls: 3, // up from 2
				// TODO: CreateAclsRequest v3 is not supported, but expected for KafkaVersion 3.3.0
				// apiKeyCreateAcls:   3, // up from 2
				// TODO: DeleteAclsRequest v3 is not supported, but expected for KafkaVersion 3.3.0
				// apiKeyDeleteAcls:   3, // up from 2
				// TODO: UpdateFeaturesRequest v1 is not supported, but expected for KafkaVersion 3.3.0
				// apiKeyUpdateFeatures /* (57) */: 1, // up from 0
			},
		},
		{
			V3_5_0_0,
			map[int16]int16{
				// TODO: FetchRequest v15 is not supported, but expected for KafkaVersion 3.5.0
				// apiKeyFetch:              15, // up from 13
				// TODO: ListOffsetsRequest v8 is not supported, but expected for KafkaVersion 3.5.0
				// apiKeyListOffsets:        8,  // up from 7
				// TODO: AddPartitionsToTxnRequest v4 is not supported, but expected for KafkaVersion 3.5.0
				// apiKeyAddPartitionsToTxn: 4,  // up from 3
				// TODO: ConsumerGroupHeartbeatRequest v0 is not supported, but expected for KafkaVersion 3.5.0
				// apiKeyConsumerGroupHeartbeat /* (68) */: 0, // new in 3.5
			},
		},
		{
			V3_6_0_0,
			map[int16]int16{
				// TODO: OffsetCommitRequest v9 is not supported, but expected for KafkaVersion 3.6.0
				// apiKeyOffsetCommit: 9, // up from 8
			},
		},
		{
			V3_7_0_0,
			map[int16]int16{
				apiKeyDescribeCluster: 1,  // up from 0
				apiKeyProduce:         10, // up from 9
				// TODO: FetchRequest v16 is not supported, but expected for KafkaVersion 3.7.0
				// apiKeyFetch:           16, // up from 15
				// TODO: OffsetFetchRequest v9 is not supported, but expected for KafkaVersion 3.7.0
				// apiKeyOffsetFetch:     9,  // up from 8
			},
		},
		{
			V3_8_0_0,
			map[int16]int16{
				apiKeyListGroups: 5, // up from 4
				// TODO: ProduceRequest v11 is not supported, but expected for KafkaVersion 3.8.0
				// apiKeyProduce:            11, // up from 10
				// TODO: FindCoordinatorRequest v5 is not supported, but expected for KafkaVersion 3.8.0
				// apiKeyFindCoordinator:    5,  // up from 4
				// TODO: InitProducerIdRequest v5 is not supported, but expected for KafkaVersion 3.8.0
				// apiKeyInitProducerId:     5,  // up from 4
				// TODO: AddPartitionsToTxnRequest v5 is not supported, but expected for KafkaVersion 3.8.0
				// apiKeyAddPartitionsToTxn: 5,  // up from 4
				// TODO: AddOffsetsToTxnRequest v4 is not supported, but expected for KafkaVersion 3.8.0
				// apiKeyAddOffsetsToTxn:    4,  // up from 3
				// TODO: EndTxnRequest v4 is not supported, but expected for KafkaVersion 3.8.0
				// apiKeyEndTxn:             4,  // up from 3
				// TODO: TxnOffsetCommitRequest v4 is not supported, but expected for KafkaVersion 3.8.0
				// apiKeyTxnOffsetCommit:    4,  // up from 3
			},
		},
		{
			V3_9_0_0,
			map[int16]int16{
				// TODO: FetchRequest v17 is not supported, but expected for KafkaVersion 3.9.0
				// apiKeyFetch:               17, // up from 16
				// TODO: ListOffsetsRequest v9 is not supported, but expected for KafkaVersion 3.9.0
				// apiKeyListOffsets:         9, // up from 8
				// TODO: FindCoordinatorRequest v6 is not supported, but expected for KafkaVersion 3.9.0
				// apiKeyFindCoordinator:     6,  // up from 5
				apiKeyApiVersions: 4, // up from 3
			},
		},
		{
			V4_0_0_0,
			map[int16]int16{
				apiKeyDescribeCluster: 2, // up from 1
				// TODO: ProduceRequest v12 is not supported, but expected for KafkaVersion 4.0.0
				// apiKeyProduce:             12, // up from 11
				// TODO: ListOffsetsRequest v10 is not supported, but expected for KafkaVersion 4.0.0
				// apiKeyListOffsets:         10, // up from 9
				// TODO: MetadataRequest v13 is not supported, but expected for KafkaVersion 4.0.0
				// apiKeyMetadata:            13, // up from 12
				// TODO: DescribeGroupsRequest v6 is not supported, but expected for KafkaVersion 4.0.0
				// apiKeyDescribeGroups:      6,  // up from 5
				// TODO: EndTxnRequest v5 is not supported, but expected for KafkaVersion 4.0.0
				// apiKeyEndTxn:              5,  // up from 4
				// TODO: TxnOffsetCommitRequest v5 is not supported, but expected for KafkaVersion 4.0.0
				// apiKeyTxnOffsetCommit:     5,  // up from 4
				// TODO: UpdateFeaturesRequest v2 is not supported, but expected for KafkaVersion 4.0.0
				// apiKeyUpdateFeatures /* (57) */: 2, // up from 1
				// TODO: ConsumerGroupHeartbeatRequest v1 is not supported, but expected for KafkaVersion 4.0.0
				// apiKeyConsumerGroupHeartbeat /* (68) */: 1, // up from 0
			},
		},
		{
			V4_1_0_0,
			map[int16]int16{
				// TODO: ProduceRequest v13 is not supported, but expected for KafkaVersion 4.1.0
				// apiKeyProduce:                     13, // up from 12
				// TODO: FetchRequest v18 is not supported, but expected for KafkaVersion 4.1.0
				// apiKeyFetch:                       18, // up from 17
				// TODO: OffsetCommitRequest v10 is not supported, but expected for KafkaVersion 4.1.0
				// apiKeyOffsetCommit:                10, // up from 9
				// TODO: OffsetFetchRequest v10 is not supported, but expected for KafkaVersion 4.1.0
				// apiKeyOffsetFetch:                 10, // up from 9
				// TODO: InitProducerIdRequest v6 is not supported, but expected for KafkaVersion 4.1.0
				// apiKeyInitProducerId:              6, // up from 5
				// TODO: AlterPartitionReassignmentsRequest v1 is not supported, but expected for KafkaVersion 4.1.0
				// apiKeyAlterPartitionReassignments: 1, // up from 0
			},
		},
		{
			V4_2_0_0,
			map[int16]int16{
				// TODO: ListOffsetsRequest v11 is not supported, but expected for KafkaVersion 4.2.0
				// apiKeyListOffsets: 11, // up from 10
			},
		},
		{
			saramaMaxVersions, // placeholder version for current maximums implemented by Sarama
			map[int16]int16{
				apiKeyProduce:                      maxVersion(&ProduceRequest{}),
				apiKeyFetch:                        maxVersion(&FetchRequest{}),
				apiKeyListOffsets:                  maxVersion(&OffsetRequest{}),
				apiKeyMetadata:                     maxVersion(&MetadataRequest{}),
				apiKeyOffsetCommit:                 maxVersion(&OffsetCommitRequest{}),
				apiKeyOffsetFetch:                  maxVersion(&OffsetFetchRequest{}),
				apiKeyFindCoordinator:              maxVersion(&FindCoordinatorRequest{}),
				apiKeyJoinGroup:                    maxVersion(&JoinGroupRequest{}),
				apiKeyHeartbeat:                    maxVersion(&HeartbeatRequest{}),
				apiKeyLeaveGroup:                   maxVersion(&LeaveGroupRequest{}),
				apiKeySyncGroup:                    maxVersion(&SyncGroupRequest{}),
				apiKeyDescribeGroups:               maxVersion(&DescribeGroupsRequest{}),
				apiKeyListGroups:                   maxVersion(&ListGroupsRequest{}),
				apiKeySaslHandshake:                maxVersion(&SaslHandshakeRequest{}),
				apiKeyApiVersions:                  maxVersion(&ApiVersionsRequest{}),
				apiKeyCreateTopics:                 maxVersion(&CreateTopicsRequest{}),
				apiKeyDeleteTopics:                 maxVersion(&DeleteTopicsRequest{}),
				apiKeyDeleteRecords:                maxVersion(&DeleteRecordsRequest{}),
				apiKeyInitProducerId:               maxVersion(&InitProducerIDRequest{}),
				apiKeyAddPartitionsToTxn:           maxVersion(&AddPartitionsToTxnRequest{}),
				apiKeyAddOffsetsToTxn:              maxVersion(&AddOffsetsToTxnRequest{}),
				apiKeyEndTxn:                       maxVersion(&EndTxnRequest{}),
				apiKeyTxnOffsetCommit:              maxVersion(&TxnOffsetCommitRequest{}),
				apiKeyDescribeAcls:                 maxVersion(&DescribeAclsRequest{}),
				apiKeyCreateAcls:                   maxVersion(&CreateAclsRequest{}),
				apiKeyDeleteAcls:                   maxVersion(&DeleteAclsRequest{}),
				apiKeyDescribeConfigs:              maxVersion(&DescribeConfigsRequest{}),
				apiKeyAlterConfigs:                 maxVersion(&AlterConfigsRequest{}),
				apiKeyDescribeLogDirs:              maxVersion(&DescribeLogDirsRequest{}),
				apiKeySASLAuth:                     maxVersion(&SaslAuthenticateRequest{}),
				apiKeyCreatePartitions:             maxVersion(&CreatePartitionsRequest{}),
				apiKeyDeleteGroups:                 maxVersion(&DeleteGroupsRequest{}),
				apiKeyElectLeaders:                 maxVersion(&ElectLeadersRequest{}),
				apiKeyIncrementalAlterConfigs:      maxVersion(&IncrementalAlterConfigsRequest{}),
				apiKeyAlterPartitionReassignments:  maxVersion(&AlterPartitionReassignmentsRequest{}),
				apiKeyListPartitionReassignments:   maxVersion(&ListPartitionReassignmentsRequest{}),
				apiKeyOffsetDelete:                 maxVersion(&DeleteOffsetsRequest{}),
				apiKeyDescribeClientQuotas:         maxVersion(&DescribeClientQuotasRequest{}),
				apiKeyAlterClientQuotas:            maxVersion(&AlterClientQuotasRequest{}),
				apiKeyDescribeUserScramCredentials: maxVersion(&DescribeUserScramCredentialsRequest{}),
				apiKeyAlterUserScramCredentials:    maxVersion(&AlterUserScramCredentialsRequest{}),
				apiKeyUpdateFeatures:               maxVersion(&UpdateFeaturesRequest{}),
				apiKeyDescribeCluster:              maxVersion(&DescribeClusterRequest{}),
			},
		},
	}

	for _, tt := range tests {
		for key, version := range tt.apiVersions {
			t.Run(fmt.Sprintf("%s-%s", tt.version.String(), names[key]), func(t *testing.T) {
				req := allocateBody(key, version)
				if req == nil {
					t.Skipf("apikey %d is not implemented", key)
				}
				t.Logf("Testing %s V%d", reflect.TypeOf(req), version)
				resp := allocateResponseBody(req.key(), req.version())
				assert.NotNil(t, resp, "%s has no matching response type in allocateResponseBody", reflect.TypeOf(req))
				assert.Equal(t, req.isValidVersion(), resp.isValidVersion(), "%s isValidVersion should match %s", reflect.TypeOf(req), reflect.TypeOf(resp))
				assert.Equal(t, req.requiredVersion(), resp.requiredVersion(), "%s requiredVersion should match %s", reflect.TypeOf(req), reflect.TypeOf(resp))
				for _, body := range []protocolBody{req, resp} {
					assert.Equal(t, key, body.key())
					assert.Equal(t, version, body.version())
					assert.True(t, body.isValidVersion(), "%s v%d is not supported, but expected for KafkaVersion %s", reflect.TypeOf(body), version, tt.version)
					assert.True(t, tt.version.IsAtLeast(body.requiredVersion()), "KafkaVersion %s should be enough for %s v%d", tt.version, reflect.TypeOf(body), version)
				}
			})
		}
	}
}

// not specific to request tests, just helper functions for testing structures that
// implement the encoder or decoder interfaces that needed somewhere to live

func testEncodable(t *testing.T, name string, in encoder, expect []byte) {
	t.Helper()
	packet, err := encode(in, nil)
	if err != nil {
		t.Error(err)
	} else if !bytes.Equal(packet, expect) {
		t.Error("Encoding", name, "failed\ngot ", packet, "\nwant", expect)
	}
}

func testDecodable(t *testing.T, name string, out decoder, in []byte) {
	t.Helper()
	err := decode(in, out, nil)
	if err != nil {
		t.Error("Decoding", name, "failed:", err)
	}
}

func testVersionDecodable(t *testing.T, name string, out versionedDecoder, in []byte, version int16) {
	t.Helper()
	err := versionedDecode(in, out, version, nil)
	if err != nil {
		t.Error("Decoding", name, "version", version, "failed:", err)
	}
}

func testRequest(t *testing.T, name string, rb protocolBody, expected []byte) {
	t.Helper()
	if !rb.requiredVersion().IsAtLeast(MinVersion) {
		t.Errorf("Request %s has invalid required version", name)
	}
	packet := testRequestEncode(t, name, rb, expected)
	testRequestDecode(t, name, rb, packet)
}

func testRequestWithoutByteComparison(t *testing.T, name string, rb protocolBody) {
	if !rb.requiredVersion().IsAtLeast(MinVersion) {
		t.Errorf("Request %s has invalid required version", name)
	}
	packet := testRequestEncode(t, name, rb, nil)
	testRequestDecode(t, name, rb, packet)
}

func testRequestEncode(t *testing.T, name string, rb protocolBody, expected []byte) []byte {
	req := &request{correlationID: 123, clientID: "foo", body: rb}
	packet, err := encode(req, nil)

	headerSize := 0

	switch rb.headerVersion() {
	case 1:
		headerSize = 14 + len("foo")
	case 2:
		headerSize = 14 + len("foo") + 1
	default:
		t.Error("Encoding", name, "failed\nheaderVersion", rb.headerVersion(), "not implemented")
	}

	if err != nil {
		t.Error(err)
	} else if expected != nil && !bytes.Equal(packet[headerSize:], expected) {
		t.Error("Encoding", name, "failed\ngot ", packet[headerSize:], "\nwant", expected)
	}
	return packet
}

func testRequestDecode(t *testing.T, name string, rb protocolBody, packet []byte) {
	t.Helper()
	decoded, n, err := decodeRequest(bytes.NewReader(packet))
	if err != nil {
		t.Error("Failed to decode request", err)
	} else if decoded.correlationID != 123 || decoded.clientID != "foo" {
		t.Errorf("Decoded header %q is not valid: %+v", name, decoded)
	} else if !reflect.DeepEqual(rb, decoded.body) {
		t.Error(spew.Sprintf("Decoded request %q does not match the encoded one\nencoded: %+v\ndecoded: %+v", name, rb, decoded.body))
	} else if n != len(packet) {
		t.Errorf("Decoded request %q bytes: %d does not match the encoded one: %d\n", name, n, len(packet))
	} else if rb.version() != decoded.body.version() {
		t.Errorf("Decoded request %q version: %d does not match the encoded one: %d\n", name, decoded.body.version(), rb.version())
	}
}

func testResponse(t *testing.T, name string, res protocolBody, expected []byte) {
	t.Helper()
	encoded, err := encode(res, nil)
	if err != nil {
		t.Error(err)
	} else if expected != nil && !bytes.Equal(encoded, expected) {
		t.Error("Encoding", name, "failed\ngot ", encoded, "\nwant", expected)
	}

	decoded := reflect.New(reflect.TypeOf(res).Elem()).Interface().(versionedDecoder)
	if err := versionedDecode(encoded, decoded, res.version(), nil); err != nil {
		t.Error("Decoding", name, "failed:", err)
	}

	if !reflect.DeepEqual(decoded, res) {
		t.Errorf("Decoded response does not match the encoded one\nencoded: %#v\ndecoded: %#v", res, decoded)
	}
}

func TestDecodeRequestErrorReturns(t *testing.T) {
	_, bytesRead, err := decodeRequest(bytes.NewReader([]byte{0, 0, 0}))
	if err == nil {
		t.Error("Decode of short request should give error but was nil")
	}
	if bytesRead != 3 {
		t.Errorf("Decode of short request should read 3 bytes but was %d", bytesRead)
	}
	_, bytesRead, err = decodeRequest(bytes.NewReader([]byte{0, 0, 0, 8, 0, 0, 0}))
	if err == nil {
		t.Error("Decode of short request should give error but was nil")
	}
	if bytesRead != 7 {
		t.Errorf("Decode of short request should read 7 bytes but was %d", bytesRead)
	}
}

func nullString(s string) *string { return &s }

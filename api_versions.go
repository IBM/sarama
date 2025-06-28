package sarama

type apiVersionRange struct {
	minVersion int16
	maxVersion int16
}

type apiVersionMap map[int16]*apiVersionRange

const (
	APIKeyProduce                      = 0
	APIKeyFetch                        = 1
	APIKeyListOffsets                  = 2
	APIKeyMetadata                     = 3
	APIKeyLeaderAndIsr                 = 4
	APIKeyStopReplica                  = 5
	APIKeyUpdateMetadata               = 6
	APIKeyControlledShutdown           = 7
	APIKeyOffsetCommit                 = 8
	APIKeyOffsetFetch                  = 9
	APIKeyFindCoordinator              = 10
	APIKeyJoinGroup                    = 11
	APIKeyHeartbeat                    = 12
	APIKeyLeaveGroup                   = 13
	APIKeySyncGroup                    = 14
	APIKeyDescribeGroups               = 15
	APIKeyListGroups                   = 16
	APIKeySaslHandshake                = 17
	APIKeyApiVersions                  = 18
	APIKeyCreateTopics                 = 19
	APIKeyDeleteTopics                 = 20
	APIKeyDeleteRecords                = 21
	APIKeyInitProducerId               = 22
	APIKeyOffsetForLeaderEpoch         = 23
	APIKeyAddPartitionsToTxn           = 24
	APIKeyAddOffsetsToTxn              = 25
	APIKeyEndTxn                       = 26
	APIKeyWriteTxnMarkers              = 27
	APIKeyTxnOffsetCommit              = 28
	APIKeyDescribeAcls                 = 29
	APIKeyCreateAcls                   = 30
	APIKeyDeleteAcls                   = 31
	APIKeyDescribeConfigs              = 32
	APIKeyAlterConfigs                 = 33
	APIKeyAlterReplicaLogDirs          = 34
	APIKeyDescribeLogDirs              = 35
	APIKeySASLAuth                     = 36
	APIKeyCreatePartitions             = 37
	APIKeyCreateDelegationToken        = 38
	APIKeyRenewDelegationToken         = 39
	APIKeyExpireDelegationToken        = 40
	APIKeyDescribeDelegationToken      = 41
	APIKeyDeleteGroups                 = 42
	APIKeyElectLeaders                 = 43
	APIKeyIncrementalAlterConfigs      = 44
	APIKeyAlterPartitionReassignments  = 45
	APIKeyListPartitionReassignments   = 46
	APIKeyOffsetDelete                 = 47
	APIKeyDescribeClientQuotas         = 48
	APIKeyAlterClientQuotas            = 49
	APIKeyDescribeUserScramCredentials = 50
	APIKeyAlterUserScramCredentials    = 51
)

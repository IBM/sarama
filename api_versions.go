package sarama

type apiVersionRange struct {
	minVersion int16
	maxVersion int16
}

type apiVersionMap map[int16]*apiVersionRange

// restrictApiVersion selects the appropriate API version for a given protocol body according to
// the client and broker version ranges. By default, it selects the maximum version supported by both
// client and broker, capped by the maximum Kafka version from Config.
// It then calls setVersion() on the protocol body.
// If no valid version is found, an error is returned.
func restrictApiVersion(pb protocolBody, brokerVersions apiVersionMap) error {
	key := pb.key()
	// Since message constructors take a Kafka version and select the maximum supported protocol version already, we can
	// rely on pb.version() being the max version supported for the user-selected Kafka API version.
	clientMax := pb.version()

	if brokerVersionRange := brokerVersions[key]; brokerVersionRange != nil {
		// Select the maximum version that both client and server support
		// Clamp to the client max to respect user preference above broker advertised version range
		pb.setVersion(min(clientMax, max(min(clientMax, brokerVersionRange.maxVersion), brokerVersionRange.minVersion)))
		return nil
	}

	return nil // no version ranges available, no restriction
}

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

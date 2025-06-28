package sarama

import (
	"encoding/binary"
	"fmt"
	"io"
)

type protocolBody interface {
	encoder
	versionedDecoder
	key() int16
	version() int16
	headerVersion() int16
	isValidVersion() bool
	requiredVersion() KafkaVersion
}

type request struct {
	correlationID int32
	clientID      string
	body          protocolBody
}

func (r *request) encode(pe packetEncoder) error {
	pe.push(&lengthField{})
	pe.putInt16(r.body.key())
	pe.putInt16(r.body.version())
	pe.putInt32(r.correlationID)

	if r.body.headerVersion() >= 1 {
		err := pe.putString(r.clientID)
		if err != nil {
			return err
		}
	}

	if r.body.headerVersion() >= 2 {
		// we don't use tag headers at the moment so we just put an array length of 0
		pe.putUVarint(0)
	}

	err := r.body.encode(pe)
	if err != nil {
		return err
	}

	return pe.pop()
}

func (r *request) decode(pd packetDecoder) (err error) {
	key, err := pd.getInt16()
	if err != nil {
		return err
	}

	version, err := pd.getInt16()
	if err != nil {
		return err
	}

	r.correlationID, err = pd.getInt32()
	if err != nil {
		return err
	}

	r.clientID, err = pd.getString()
	if err != nil {
		return err
	}

	r.body = allocateBody(key, version)
	if r.body == nil {
		return PacketDecodingError{fmt.Sprintf("unknown request key (%d)", key)}
	}

	if r.body.headerVersion() >= 2 {
		// tagged field
		_, err = pd.getUVarint()
		if err != nil {
			return err
		}
	}

	return r.body.decode(pd, version)
}

func decodeRequest(r io.Reader) (*request, int, error) {
	var (
		bytesRead   int
		lengthBytes = make([]byte, 4)
	)

	if _, err := io.ReadFull(r, lengthBytes); err != nil {
		return nil, bytesRead, err
	}

	bytesRead += len(lengthBytes)
	length := int32(binary.BigEndian.Uint32(lengthBytes))

	if length <= 4 || length > MaxRequestSize {
		return nil, bytesRead, PacketDecodingError{fmt.Sprintf("message of length %d too large or too small", length)}
	}

	encodedReq := make([]byte, length)
	if _, err := io.ReadFull(r, encodedReq); err != nil {
		return nil, bytesRead, err
	}

	bytesRead += len(encodedReq)

	req := &request{}
	if err := decode(encodedReq, req, nil); err != nil {
		return nil, bytesRead, err
	}

	return req, bytesRead, nil
}

func allocateBody(key, version int16) protocolBody {
	switch key {
	case APIKeyProduce:
		return &ProduceRequest{Version: version}
	case APIKeyFetch:
		return &FetchRequest{Version: version}
	case APIKeyListOffsets:
		return &OffsetRequest{Version: version}
	case APIKeyMetadata:
		return &MetadataRequest{Version: version}
	// 4: LeaderAndIsrRequest
	// 5: StopReplicaRequest
	// 6: UpdateMetadataRequest
	// 7: ControlledShutdownRequest
	case APIKeyOffsetCommit:
		return &OffsetCommitRequest{Version: version}
	case APIKeyOffsetFetch:
		return &OffsetFetchRequest{Version: version}
	case APIKeyFindCoordinator:
		return &FindCoordinatorRequest{Version: version}
	case APIKeyJoinGroup:
		return &JoinGroupRequest{Version: version}
	case APIKeyHeartbeat:
		return &HeartbeatRequest{Version: version}
	case APIKeyLeaveGroup:
		return &LeaveGroupRequest{Version: version}
	case APIKeySyncGroup:
		return &SyncGroupRequest{Version: version}
	case APIKeyDescribeGroups:
		return &DescribeGroupsRequest{Version: version}
	case APIKeyListGroups:
		return &ListGroupsRequest{Version: version}
	case APIKeySaslHandshake:
		return &SaslHandshakeRequest{Version: version}
	case APIKeyApiVersions:
		return &ApiVersionsRequest{Version: version}
	case APIKeyCreateTopics:
		return &CreateTopicsRequest{Version: version}
	case APIKeyDeleteTopics:
		return &DeleteTopicsRequest{Version: version}
	case APIKeyDeleteRecords:
		return &DeleteRecordsRequest{Version: version}
	case APIKeyInitProducerId:
		return &InitProducerIDRequest{Version: version}
	// 23: OffsetForLeaderEpochRequest
	case APIKeyAddPartitionsToTxn:
		return &AddPartitionsToTxnRequest{Version: version}
	case APIKeyAddOffsetsToTxn:
		return &AddOffsetsToTxnRequest{Version: version}
	case APIKeyEndTxn:
		return &EndTxnRequest{Version: version}
	// 27: WriteTxnMarkersRequest
	case APIKeyTxnOffsetCommit:
		return &TxnOffsetCommitRequest{Version: version}
	case APIKeyDescribeAcls:
		return &DescribeAclsRequest{Version: int(version)}
	case APIKeyCreateAcls:
		return &CreateAclsRequest{Version: version}
	case APIKeyDeleteAcls:
		return &DeleteAclsRequest{Version: int(version)}
	case APIKeyDescribeConfigs:
		return &DescribeConfigsRequest{Version: version}
	case APIKeyAlterConfigs:
		return &AlterConfigsRequest{Version: version}
	// 34: AlterReplicaLogDirsRequest
	case APIKeyDescribeLogDirs:
		return &DescribeLogDirsRequest{Version: version}
	case APIKeySASLAuth:
		return &SaslAuthenticateRequest{Version: version}
	case APIKeyCreatePartitions:
		return &CreatePartitionsRequest{Version: version}
	// 38: CreateDelegationTokenRequest
	// 39: RenewDelegationTokenRequest
	// 40: ExpireDelegationTokenRequest
	// 41: DescribeDelegationTokenRequest
	case APIKeyDeleteGroups:
		return &DeleteGroupsRequest{Version: version}
	case APIKeyElectLeaders:
		return &ElectLeadersRequest{Version: version}
	case APIKeyIncrementalAlterConfigs:
		return &IncrementalAlterConfigsRequest{Version: version}
	case APIKeyAlterPartitionReassignments:
		return &AlterPartitionReassignmentsRequest{Version: version}
	case APIKeyListPartitionReassignments:
		return &ListPartitionReassignmentsRequest{Version: version}
	case APIKeyOffsetDelete:
		return &DeleteOffsetsRequest{Version: version}
	case APIKeyDescribeClientQuotas:
		return &DescribeClientQuotasRequest{Version: version}
	case APIKeyAlterClientQuotas:
		return &AlterClientQuotasRequest{Version: version}
	case APIKeyDescribeUserScramCredentials:
		return &DescribeUserScramCredentialsRequest{Version: version}
	case APIKeyAlterUserScramCredentials:
		return &AlterUserScramCredentialsRequest{Version: version}
		// 52: VoteRequest
		// 53: BeginQuorumEpochRequest
		// 54: EndQuorumEpochRequest
		// 55: DescribeQuorumRequest
		// 56: AlterPartitionRequest
		// 57: UpdateFeaturesRequest
		// 58: EnvelopeRequest
		// 59: FetchSnapshotRequest
		// 60: DescribeClusterRequest
		// 61: DescribeProducersRequest
		// 62: BrokerRegistrationRequest
		// 63: BrokerHeartbeatRequest
		// 64: UnregisterBrokerRequest
		// 65: DescribeTransactionsRequest
		// 66: ListTransactionsRequest
		// 67: AllocateProducerIdsRequest
		// 68: ConsumerGroupHeartbeatRequest
	}
	return nil
}

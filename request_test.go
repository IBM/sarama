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

var names = map[int16]string{
	APIKeyProduce:                      "ProduceRequest",
	APIKeyFetch:                        "FetchRequest",
	APIKeyListOffsets:                  "ListOffsetsRequest",
	APIKeyMetadata:                     "MetadataRequest",
	APIKeyLeaderAndIsr:                 "LeaderAndIsrRequest",
	APIKeyStopReplica:                  "StopReplicaRequest",
	APIKeyUpdateMetadata:               "UpdateMetadataRequest",
	APIKeyControlledShutdown:           "ControlledShutdownRequest",
	APIKeyOffsetCommit:                 "OffsetCommitRequest",
	APIKeyOffsetFetch:                  "OffsetFetchRequest",
	APIKeyFindCoordinator:              "FindCoordinatorRequest",
	APIKeyJoinGroup:                    "JoinGroupRequest",
	APIKeyHeartbeat:                    "HeartbeatRequest",
	APIKeyLeaveGroup:                   "LeaveGroupRequest",
	APIKeySyncGroup:                    "SyncGroupRequest",
	APIKeyDescribeGroups:               "DescribeGroupsRequest",
	APIKeyListGroups:                   "ListGroupsRequest",
	APIKeySaslHandshake:                "SaslHandshakeRequest",
	APIKeyApiVersions:                  "ApiVersionsRequest",
	APIKeyCreateTopics:                 "CreateTopicsRequest",
	APIKeyDeleteTopics:                 "DeleteTopicsRequest",
	APIKeyDeleteRecords:                "DeleteRecordsRequest",
	APIKeyInitProducerId:               "InitProducerIdRequest",
	APIKeyOffsetForLeaderEpoch:         "OffsetForLeaderEpochRequest",
	APIKeyAddPartitionsToTxn:           "AddPartitionsToTxnRequest",
	APIKeyAddOffsetsToTxn:              "AddOffsetsToTxnRequest",
	APIKeyEndTxn:                       "EndTxnRequest",
	APIKeyWriteTxnMarkers:              "WriteTxnMarkersRequest",
	APIKeyTxnOffsetCommit:              "TxnOffsetCommitRequest",
	APIKeyDescribeAcls:                 "DescribeAclsRequest",
	APIKeyCreateAcls:                   "CreateAclsRequest",
	APIKeyDeleteAcls:                   "DeleteAclsRequest",
	APIKeyDescribeConfigs:              "DescribeConfigsRequest",
	APIKeyAlterConfigs:                 "AlterConfigsRequest",
	APIKeyAlterReplicaLogDirs:          "AlterReplicaLogDirsRequest",
	APIKeyDescribeLogDirs:              "DescribeLogDirsRequest",
	APIKeySASLAuth:                     "SaslAuthenticateRequest",
	APIKeyCreatePartitions:             "CreatePartitionsRequest",
	APIKeyCreateDelegationToken:        "CreateDelegationTokenRequest",
	APIKeyRenewDelegationToken:         "RenewDelegationTokenRequest",
	APIKeyExpireDelegationToken:        "ExpireDelegationTokenRequest",
	APIKeyDescribeDelegationToken:      "DescribeDelegationTokenRequest",
	APIKeyDeleteGroups:                 "DeleteGroupsRequest",
	APIKeyElectLeaders:                 "ElectLeadersRequest",
	APIKeyIncrementalAlterConfigs:      "IncrementalAlterConfigsRequest",
	APIKeyAlterPartitionReassignments:  "AlterPartitionReassignmentsRequest",
	APIKeyListPartitionReassignments:   "ListPartitionReassignmentsRequest",
	APIKeyOffsetDelete:                 "OffsetDeleteRequest",
	APIKeyDescribeClientQuotas:         "DescribeClientQuotasRequest",
	APIKeyAlterClientQuotas:            "AlterClientQuotasRequest",
	APIKeyDescribeUserScramCredentials: "DescribeUserScramCredentialsRequest",
	APIKeyAlterUserScramCredentials:    "AlterUserScramCredentialsRequest",
	52:                                 "VoteRequest",
	53:                                 "BeginQuorumEpochRequest",
	54:                                 "EndQuorumEpochRequest",
	55:                                 "DescribeQuorumRequest",
	56:                                 "AlterPartitionRequest",
	57:                                 "UpdateFeaturesRequest",
	58:                                 "EnvelopeRequest",
	59:                                 "FetchSnapshotRequest",
	60:                                 "DescribeClusterRequest",
	61:                                 "DescribeProducersRequest",
	62:                                 "BrokerRegistrationRequest",
	63:                                 "BrokerHeartbeatRequest",
	64:                                 "UnregisterBrokerRequest",
	65:                                 "DescribeTransactionsRequest",
	66:                                 "ListTransactionsRequest",
	67:                                 "AllocateProducerIdsRequest",
	68:                                 "ConsumerGroupHeartbeatRequest",
}

// allocateResponseBody is a test-only clone of allocateBody. There's no
// central registry of types, so we can't do this using reflection for Response
// types and assuming that the struct is identically named, just with Response
// instead of Request.
func allocateResponseBody(req protocolBody) protocolBody {
	key := req.key()
	version := req.version()
	switch key {
	case APIKeyProduce:
		return &ProduceResponse{Version: version}
	case APIKeyFetch:
		return &FetchResponse{Version: version}
	case APIKeyListOffsets:
		return &OffsetResponse{Version: version}
	case APIKeyMetadata:
		return &MetadataResponse{Version: version}
	case APIKeyOffsetCommit:
		return &OffsetCommitResponse{Version: version}
	case APIKeyOffsetFetch:
		return &OffsetFetchResponse{Version: version}
	case APIKeyFindCoordinator:
		return &FindCoordinatorResponse{Version: version}
	case APIKeyJoinGroup:
		return &JoinGroupResponse{Version: version}
	case APIKeyHeartbeat:
		return &HeartbeatResponse{Version: version}
	case APIKeyLeaveGroup:
		return &LeaveGroupResponse{Version: version}
	case APIKeySyncGroup:
		return &SyncGroupResponse{Version: version}
	case APIKeyDescribeGroups:
		return &DescribeGroupsResponse{Version: version}
	case APIKeyListGroups:
		return &ListGroupsResponse{Version: version}
	case APIKeySaslHandshake:
		return &SaslHandshakeResponse{Version: version}
	case APIKeyApiVersions:
		return &ApiVersionsResponse{Version: version}
	case APIKeyCreateTopics:
		return &CreateTopicsResponse{Version: version}
	case APIKeyDeleteTopics:
		return &DeleteTopicsResponse{Version: version}
	case APIKeyDeleteRecords:
		return &DeleteRecordsResponse{Version: version}
	case APIKeyInitProducerId:
		return &InitProducerIDResponse{Version: version}
	case APIKeyAddPartitionsToTxn:
		return &AddPartitionsToTxnResponse{Version: version}
	case APIKeyAddOffsetsToTxn:
		return &AddOffsetsToTxnResponse{Version: version}
	case APIKeyEndTxn:
		return &EndTxnResponse{Version: version}
	case APIKeyTxnOffsetCommit:
		return &TxnOffsetCommitResponse{Version: version}
	case APIKeyDescribeAcls:
		return &DescribeAclsResponse{Version: version}
	case APIKeyCreateAcls:
		return &CreateAclsResponse{Version: version}
	case APIKeyDeleteAcls:
		return &DeleteAclsResponse{Version: version}
	case APIKeyDescribeConfigs:
		return &DescribeConfigsResponse{Version: version}
	case APIKeyAlterConfigs:
		return &AlterConfigsResponse{Version: version}
	case APIKeyDescribeLogDirs:
		return &DescribeLogDirsResponse{Version: version}
	case APIKeySASLAuth:
		return &SaslAuthenticateResponse{Version: version}
	case APIKeyCreatePartitions:
		return &CreatePartitionsResponse{Version: version}
	case APIKeyDeleteGroups:
		return &DeleteGroupsResponse{Version: version}
	case APIKeyIncrementalAlterConfigs:
		return &IncrementalAlterConfigsResponse{Version: version}
	case APIKeyAlterPartitionReassignments:
		return &AlterPartitionReassignmentsResponse{Version: version}
	case APIKeyListPartitionReassignments:
		return &ListPartitionReassignmentsResponse{Version: version}
	case APIKeyOffsetDelete:
		return &DeleteOffsetsResponse{Version: version}
	case APIKeyDescribeClientQuotas:
		return &DescribeClientQuotasResponse{Version: version}
	case APIKeyAlterClientQuotas:
		return &AlterClientQuotasResponse{Version: version}
	case APIKeyDescribeUserScramCredentials:
		return &DescribeUserScramCredentialsResponse{Version: version}
	case APIKeyAlterUserScramCredentials:
		return &AlterUserScramCredentialsResponse{Version: version}
	}
	return nil
}

func TestAllocateBodyProtocolVersions(t *testing.T) {
	type test struct {
		version     KafkaVersion
		apiVersions map[int16]int16
	}

	tests := []test{
		{
			V1_1_0_0,
			map[int16]int16{
				APIKeyProduce:                 5,
				APIKeyFetch:                   7,
				APIKeyListOffsets:             2,
				APIKeyMetadata:                5,
				APIKeyLeaderAndIsr:            1,
				APIKeyStopReplica:             0,
				APIKeyUpdateMetadata:          4,
				APIKeyControlledShutdown:      1,
				APIKeyOffsetCommit:            3,
				APIKeyOffsetFetch:             3,
				APIKeyFindCoordinator:         1,
				APIKeyJoinGroup:               2,
				APIKeyHeartbeat:               1,
				APIKeyLeaveGroup:              1,
				APIKeySyncGroup:               1,
				APIKeyDescribeGroups:          1,
				APIKeyListGroups:              1,
				APIKeySaslHandshake:           1,
				APIKeyApiVersions:             1,
				APIKeyCreateTopics:            2,
				APIKeyDeleteTopics:            1,
				APIKeyDeleteRecords:           0,
				APIKeyInitProducerId:          0,
				APIKeyOffsetForLeaderEpoch:    0,
				APIKeyAddPartitionsToTxn:      0,
				APIKeyAddOffsetsToTxn:         0,
				APIKeyEndTxn:                  0,
				APIKeyWriteTxnMarkers:         0,
				APIKeyTxnOffsetCommit:         0,
				APIKeyDescribeAcls:            0,
				APIKeyCreateAcls:              0,
				APIKeyDeleteAcls:              0,
				APIKeyDescribeConfigs:         1,
				APIKeyAlterConfigs:            0,
				APIKeyAlterReplicaLogDirs:     0,
				APIKeyDescribeLogDirs:         0,
				APIKeySASLAuth:                0,
				APIKeyCreatePartitions:        0,
				APIKeyCreateDelegationToken:   0,
				APIKeyRenewDelegationToken:    0,
				APIKeyExpireDelegationToken:   0,
				APIKeyDescribeDelegationToken: 0,
				APIKeyDeleteGroups:            0,
			},
		},
		{
			V2_0_0_0,
			map[int16]int16{
				APIKeyProduce:                 6,
				APIKeyFetch:                   8,
				APIKeyListOffsets:             3,
				APIKeyMetadata:                6,
				APIKeyLeaderAndIsr:            1,
				APIKeyStopReplica:             0,
				APIKeyUpdateMetadata:          4,
				APIKeyControlledShutdown:      1,
				APIKeyOffsetCommit:            4,
				APIKeyOffsetFetch:             4,
				APIKeyFindCoordinator:         2,
				APIKeyJoinGroup:               3,
				APIKeyHeartbeat:               2,
				APIKeyLeaveGroup:              2,
				APIKeySyncGroup:               2,
				APIKeyDescribeGroups:          2,
				APIKeyListGroups:              2,
				APIKeySaslHandshake:           1,
				APIKeyApiVersions:             2,
				APIKeyCreateTopics:            3,
				APIKeyDeleteTopics:            2,
				APIKeyDeleteRecords:           1,
				APIKeyInitProducerId:          1,
				APIKeyOffsetForLeaderEpoch:    1,
				APIKeyAddPartitionsToTxn:      1,
				APIKeyAddOffsetsToTxn:         1,
				APIKeyEndTxn:                  1,
				APIKeyWriteTxnMarkers:         0,
				APIKeyTxnOffsetCommit:         1,
				APIKeyDescribeAcls:            1,
				APIKeyCreateAcls:              1,
				APIKeyDeleteAcls:              1,
				APIKeyDescribeConfigs:         2,
				APIKeyAlterConfigs:            1,
				APIKeyAlterReplicaLogDirs:     1,
				APIKeyDescribeLogDirs:         1,
				APIKeySASLAuth:                0,
				APIKeyCreatePartitions:        1,
				APIKeyCreateDelegationToken:   1,
				APIKeyRenewDelegationToken:    1,
				APIKeyExpireDelegationToken:   1,
				APIKeyDescribeDelegationToken: 1,
				APIKeyDeleteGroups:            1,
			},
		},
		{
			V2_1_0_0,
			map[int16]int16{
				APIKeyProduce:                 7,
				APIKeyFetch:                   10,
				APIKeyListOffsets:             4,
				APIKeyMetadata:                7,
				APIKeyLeaderAndIsr:            1,
				APIKeyStopReplica:             0,
				APIKeyUpdateMetadata:          4,
				APIKeyControlledShutdown:      1,
				APIKeyOffsetCommit:            6,
				APIKeyOffsetFetch:             5,
				APIKeyFindCoordinator:         2,
				APIKeyJoinGroup:               3,
				APIKeyHeartbeat:               2,
				APIKeyLeaveGroup:              2,
				APIKeySyncGroup:               2,
				APIKeyDescribeGroups:          2,
				APIKeyListGroups:              2,
				APIKeySaslHandshake:           1,
				APIKeyApiVersions:             2,
				APIKeyCreateTopics:            3,
				APIKeyDeleteTopics:            3,
				APIKeyDeleteRecords:           1,
				APIKeyInitProducerId:          1,
				APIKeyOffsetForLeaderEpoch:    2,
				APIKeyAddPartitionsToTxn:      1,
				APIKeyAddOffsetsToTxn:         1,
				APIKeyEndTxn:                  1,
				APIKeyWriteTxnMarkers:         0,
				APIKeyTxnOffsetCommit:         2,
				APIKeyDescribeAcls:            1,
				APIKeyCreateAcls:              1,
				APIKeyDeleteAcls:              1,
				APIKeyDescribeConfigs:         2,
				APIKeyAlterConfigs:            1,
				APIKeyAlterReplicaLogDirs:     1,
				APIKeyDescribeLogDirs:         1,
				APIKeySASLAuth:                0,
				APIKeyCreatePartitions:        1,
				APIKeyCreateDelegationToken:   1,
				APIKeyRenewDelegationToken:    1,
				APIKeyExpireDelegationToken:   1,
				APIKeyDescribeDelegationToken: 1,
				APIKeyDeleteGroups:            1,
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
				resp := allocateResponseBody(req)
				assert.NotNil(t, resp, fmt.Sprintf("%s has no matching response type in allocateResponseBody", reflect.TypeOf(req)))
				assert.Equal(t, req.isValidVersion(), resp.isValidVersion(), fmt.Sprintf("%s isValidVersion should match %s", reflect.TypeOf(req), reflect.TypeOf(resp)))
				assert.Equal(t, req.requiredVersion(), resp.requiredVersion(), fmt.Sprintf("%s requiredVersion should match %s", reflect.TypeOf(req), reflect.TypeOf(resp)))
				for _, body := range []protocolBody{req, resp} {
					assert.Equal(t, key, body.key())
					assert.Equal(t, version, body.version())
					assert.True(t, body.isValidVersion(), fmt.Sprintf("%s v%d is not supported, but expected for KafkaVersion %s", reflect.TypeOf(body), version, tt.version))
					assert.True(t, tt.version.IsAtLeast(body.requiredVersion()), fmt.Sprintf("KafkaVersion %s should be enough for %s v%d", tt.version, reflect.TypeOf(body), version))
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

func nullString(s string) *string { return &s }

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
	case apiKeyProduce:
		return &ProduceResponse{Version: version}
	case apiKeyFetch:
		return &FetchResponse{Version: version}
	case apiKeyListOffsets:
		return &OffsetResponse{Version: version}
	case apiKeyMetadata:
		return &MetadataResponse{Version: version}
	case apiKeyOffsetCommit:
		return &OffsetCommitResponse{Version: version}
	case apiKeyOffsetFetch:
		return &OffsetFetchResponse{Version: version}
	case apiKeyFindCoordinator:
		return &FindCoordinatorResponse{Version: version}
	case apiKeyJoinGroup:
		return &JoinGroupResponse{Version: version}
	case apiKeyHeartbeat:
		return &HeartbeatResponse{Version: version}
	case apiKeyLeaveGroup:
		return &LeaveGroupResponse{Version: version}
	case apiKeySyncGroup:
		return &SyncGroupResponse{Version: version}
	case apiKeyDescribeGroups:
		return &DescribeGroupsResponse{Version: version}
	case apiKeyListGroups:
		return &ListGroupsResponse{Version: version}
	case apiKeySaslHandshake:
		return &SaslHandshakeResponse{Version: version}
	case apiKeyApiVersions:
		return &ApiVersionsResponse{Version: version}
	case apiKeyCreateTopics:
		return &CreateTopicsResponse{Version: version}
	case apiKeyDeleteTopics:
		return &DeleteTopicsResponse{Version: version}
	case apiKeyDeleteRecords:
		return &DeleteRecordsResponse{Version: version}
	case apiKeyInitProducerId:
		return &InitProducerIDResponse{Version: version}
	case apiKeyAddPartitionsToTxn:
		return &AddPartitionsToTxnResponse{Version: version}
	case apiKeyAddOffsetsToTxn:
		return &AddOffsetsToTxnResponse{Version: version}
	case apiKeyEndTxn:
		return &EndTxnResponse{Version: version}
	case apiKeyTxnOffsetCommit:
		return &TxnOffsetCommitResponse{Version: version}
	case apiKeyDescribeAcls:
		return &DescribeAclsResponse{Version: version}
	case apiKeyCreateAcls:
		return &CreateAclsResponse{Version: version}
	case apiKeyDeleteAcls:
		return &DeleteAclsResponse{Version: version}
	case apiKeyDescribeConfigs:
		return &DescribeConfigsResponse{Version: version}
	case apiKeyAlterConfigs:
		return &AlterConfigsResponse{Version: version}
	case apiKeyDescribeLogDirs:
		return &DescribeLogDirsResponse{Version: version}
	case apiKeySASLAuth:
		return &SaslAuthenticateResponse{Version: version}
	case apiKeyCreatePartitions:
		return &CreatePartitionsResponse{Version: version}
	case apiKeyDeleteGroups:
		return &DeleteGroupsResponse{Version: version}
	case apiKeyIncrementalAlterConfigs:
		return &IncrementalAlterConfigsResponse{Version: version}
	case apiKeyAlterPartitionReassignments:
		return &AlterPartitionReassignmentsResponse{Version: version}
	case apiKeyListPartitionReassignments:
		return &ListPartitionReassignmentsResponse{Version: version}
	case apiKeyOffsetDelete:
		return &DeleteOffsetsResponse{Version: version}
	case apiKeyDescribeClientQuotas:
		return &DescribeClientQuotasResponse{Version: version}
	case apiKeyAlterClientQuotas:
		return &AlterClientQuotasResponse{Version: version}
	case apiKeyDescribeUserScramCredentials:
		return &DescribeUserScramCredentialsResponse{Version: version}
	case apiKeyAlterUserScramCredentials:
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
				apiKeyProduce:                 6,
				apiKeyFetch:                   8,
				apiKeyListOffsets:             3,
				apiKeyMetadata:                6,
				apiKeyLeaderAndIsr:            1,
				apiKeyStopReplica:             0,
				apiKeyUpdateMetadata:          4,
				apiKeyControlledShutdown:      1,
				apiKeyOffsetCommit:            4,
				apiKeyOffsetFetch:             4,
				apiKeyFindCoordinator:         2,
				apiKeyJoinGroup:               3,
				apiKeyHeartbeat:               2,
				apiKeyLeaveGroup:              2,
				apiKeySyncGroup:               2,
				apiKeyDescribeGroups:          2,
				apiKeyListGroups:              2,
				apiKeySaslHandshake:           1,
				apiKeyApiVersions:             2,
				apiKeyCreateTopics:            3,
				apiKeyDeleteTopics:            2,
				apiKeyDeleteRecords:           1,
				apiKeyInitProducerId:          1,
				apiKeyOffsetForLeaderEpoch:    1,
				apiKeyAddPartitionsToTxn:      1,
				apiKeyAddOffsetsToTxn:         1,
				apiKeyEndTxn:                  1,
				apiKeyWriteTxnMarkers:         0,
				apiKeyTxnOffsetCommit:         1,
				apiKeyDescribeAcls:            1,
				apiKeyCreateAcls:              1,
				apiKeyDeleteAcls:              1,
				apiKeyDescribeConfigs:         2,
				apiKeyAlterConfigs:            1,
				apiKeyAlterReplicaLogDirs:     1,
				apiKeyDescribeLogDirs:         1,
				apiKeySASLAuth:                0,
				apiKeyCreatePartitions:        1,
				apiKeyCreateDelegationToken:   1,
				apiKeyRenewDelegationToken:    1,
				apiKeyExpireDelegationToken:   1,
				apiKeyDescribeDelegationToken: 1,
				apiKeyDeleteGroups:            1,
			},
		},
		{
			V2_1_0_0,
			map[int16]int16{
				apiKeyProduce:                 7,
				apiKeyFetch:                   10,
				apiKeyListOffsets:             4,
				apiKeyMetadata:                7,
				apiKeyLeaderAndIsr:            1,
				apiKeyStopReplica:             0,
				apiKeyUpdateMetadata:          4,
				apiKeyControlledShutdown:      1,
				apiKeyOffsetCommit:            6,
				apiKeyOffsetFetch:             5,
				apiKeyFindCoordinator:         2,
				apiKeyJoinGroup:               3,
				apiKeyHeartbeat:               2,
				apiKeyLeaveGroup:              2,
				apiKeySyncGroup:               2,
				apiKeyDescribeGroups:          2,
				apiKeyListGroups:              2,
				apiKeySaslHandshake:           1,
				apiKeyApiVersions:             2,
				apiKeyCreateTopics:            3,
				apiKeyDeleteTopics:            3,
				apiKeyDeleteRecords:           1,
				apiKeyInitProducerId:          1,
				apiKeyOffsetForLeaderEpoch:    2,
				apiKeyAddPartitionsToTxn:      1,
				apiKeyAddOffsetsToTxn:         1,
				apiKeyEndTxn:                  1,
				apiKeyWriteTxnMarkers:         0,
				apiKeyTxnOffsetCommit:         2,
				apiKeyDescribeAcls:            1,
				apiKeyCreateAcls:              1,
				apiKeyDeleteAcls:              1,
				apiKeyDescribeConfigs:         2,
				apiKeyAlterConfigs:            1,
				apiKeyAlterReplicaLogDirs:     1,
				apiKeyDescribeLogDirs:         1,
				apiKeySASLAuth:                0,
				apiKeyCreatePartitions:        1,
				apiKeyCreateDelegationToken:   1,
				apiKeyRenewDelegationToken:    1,
				apiKeyExpireDelegationToken:   1,
				apiKeyDescribeDelegationToken: 1,
				apiKeyDeleteGroups:            1,
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

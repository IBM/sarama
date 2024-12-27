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
	0:  "ProduceRequest",
	1:  "FetchRequest",
	2:  "ListOffsetsRequest",
	3:  "MetadataRequest",
	4:  "LeaderAndIsrRequest",
	5:  "StopReplicaRequest",
	6:  "UpdateMetadataRequest",
	7:  "ControlledShutdownRequest",
	8:  "OffsetCommitRequest",
	9:  "OffsetFetchRequest",
	10: "FindCoordinatorRequest",
	11: "JoinGroupRequest",
	12: "HeartbeatRequest",
	13: "LeaveGroupRequest",
	14: "SyncGroupRequest",
	15: "DescribeGroupsRequest",
	16: "ListGroupsRequest",
	17: "SaslHandshakeRequest",
	18: "ApiVersionsRequest",
	19: "CreateTopicsRequest",
	20: "DeleteTopicsRequest",
	21: "DeleteRecordsRequest",
	22: "InitProducerIdRequest",
	23: "OffsetForLeaderEpochRequest",
	24: "AddPartitionsToTxnRequest",
	25: "AddOffsetsToTxnRequest",
	26: "EndTxnRequest",
	27: "WriteTxnMarkersRequest",
	28: "TxnOffsetCommitRequest",
	29: "DescribeAclsRequest",
	30: "CreateAclsRequest",
	31: "DeleteAclsRequest",
	32: "DescribeConfigsRequest",
	33: "AlterConfigsRequest",
	34: "AlterReplicaLogDirsRequest",
	35: "DescribeLogDirsRequest",
	36: "SaslAuthenticateRequest",
	37: "CreatePartitionsRequest",
	38: "CreateDelegationTokenRequest",
	39: "RenewDelegationTokenRequest",
	40: "ExpireDelegationTokenRequest",
	41: "DescribeDelegationTokenRequest",
	42: "DeleteGroupsRequest",
	43: "ElectLeadersRequest",
	44: "IncrementalAlterConfigsRequest",
	45: "AlterPartitionReassignmentsRequest",
	46: "ListPartitionReassignmentsRequest",
	47: "OffsetDeleteRequest",
	48: "DescribeClientQuotasRequest",
	49: "AlterClientQuotasRequest",
	50: "DescribeUserScramCredentialsRequest",
	51: "AlterUserScramCredentialsRequest",
	52: "VoteRequest",
	53: "BeginQuorumEpochRequest",
	54: "EndQuorumEpochRequest",
	55: "DescribeQuorumRequest",
	56: "AlterPartitionRequest",
	57: "UpdateFeaturesRequest",
	58: "EnvelopeRequest",
	59: "FetchSnapshotRequest",
	60: "DescribeClusterRequest",
	61: "DescribeProducersRequest",
	62: "BrokerRegistrationRequest",
	63: "BrokerHeartbeatRequest",
	64: "UnregisterBrokerRequest",
	65: "DescribeTransactionsRequest",
	66: "ListTransactionsRequest",
	67: "AllocateProducerIdsRequest",
	68: "ConsumerGroupHeartbeatRequest",
}

// allocateResponseBody is a test-only clone of allocateBody. There's no
// central registry of types, so we can't do this using reflection for Response
// types and assuming that the struct is identically named, just with Response
// instead of Request.
func allocateResponseBody(req protocolBody) protocolBody {
	key := req.key()
	version := req.version()
	switch key {
	case 0:
		return &ProduceResponse{Version: version}
	case 1:
		return &FetchResponse{Version: version}
	case 2:
		return &OffsetResponse{Version: version}
	case 3:
		return &MetadataResponse{Version: version}
	case 8:
		return &OffsetCommitResponse{Version: version}
	case 9:
		return &OffsetFetchResponse{Version: version}
	case 10:
		return &FindCoordinatorResponse{Version: version}
	case 11:
		return &JoinGroupResponse{Version: version}
	case 12:
		return &HeartbeatResponse{Version: version}
	case 13:
		return &LeaveGroupResponse{Version: version}
	case 14:
		return &SyncGroupResponse{Version: version}
	case 15:
		return &DescribeGroupsResponse{Version: version}
	case 16:
		return &ListGroupsResponse{Version: version}
	case 17:
		return &SaslHandshakeResponse{Version: version}
	case 18:
		return &ApiVersionsResponse{Version: version}
	case 19:
		return &CreateTopicsResponse{Version: version}
	case 20:
		return &DeleteTopicsResponse{Version: version}
	case 21:
		return &DeleteRecordsResponse{Version: version}
	case 22:
		return &InitProducerIDResponse{Version: version}
	case 24:
		return &AddPartitionsToTxnResponse{Version: version}
	case 25:
		return &AddOffsetsToTxnResponse{Version: version}
	case 26:
		return &EndTxnResponse{Version: version}
	case 28:
		return &TxnOffsetCommitResponse{Version: version}
	case 29:
		return &DescribeAclsResponse{Version: version}
	case 30:
		return &CreateAclsResponse{Version: version}
	case 31:
		return &DeleteAclsResponse{Version: version}
	case 32:
		return &DescribeConfigsResponse{Version: version}
	case 33:
		return &AlterConfigsResponse{Version: version}
	case 35:
		return &DescribeLogDirsResponse{Version: version}
	case 36:
		return &SaslAuthenticateResponse{Version: version}
	case 37:
		return &CreatePartitionsResponse{Version: version}
	case 42:
		return &DeleteGroupsResponse{Version: version}
	case 44:
		return &IncrementalAlterConfigsResponse{Version: version}
	case 45:
		return &AlterPartitionReassignmentsResponse{Version: version}
	case 46:
		return &ListPartitionReassignmentsResponse{Version: version}
	case 47:
		return &DeleteOffsetsResponse{Version: version}
	case 48:
		return &DescribeClientQuotasResponse{Version: version}
	case 49:
		return &AlterClientQuotasResponse{Version: version}
	case 50:
		return &DescribeUserScramCredentialsResponse{Version: version}
	case 51:
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
				0:  5,
				1:  7,
				2:  2,
				3:  5,
				4:  1,
				5:  0,
				6:  4,
				7:  1,
				8:  3,
				9:  3,
				10: 1,
				11: 2,
				12: 1,
				13: 1,
				14: 1,
				15: 1,
				16: 1,
				17: 1,
				18: 1,
				19: 2,
				20: 1,
				21: 0,
				22: 0,
				23: 0,
				24: 0,
				25: 0,
				26: 0,
				27: 0,
				28: 0,
				29: 0,
				30: 0,
				31: 0,
				32: 1,
				33: 0,
				34: 0,
				35: 0,
				36: 0,
				37: 0,
				38: 0,
				39: 0,
				40: 0,
				41: 0,
				42: 0,
			},
		},
		{
			V2_0_0_0,
			map[int16]int16{
				0:  6,
				1:  8,
				2:  3,
				3:  6,
				4:  1,
				5:  0,
				6:  4,
				7:  1,
				8:  4,
				9:  4,
				10: 2,
				11: 3,
				12: 2,
				13: 2,
				14: 2,
				15: 2,
				16: 2,
				17: 1,
				18: 2,
				19: 3,
				20: 2,
				21: 1,
				22: 1,
				23: 1,
				24: 1,
				25: 1,
				26: 1,
				27: 0,
				28: 1,
				29: 1,
				30: 1,
				31: 1,
				32: 2,
				33: 1,
				34: 1,
				35: 1,
				36: 0,
				37: 1,
				38: 1,
				39: 1,
				40: 1,
				41: 1,
				42: 1,
			},
		},
		{
			V2_1_0_0,
			map[int16]int16{
				0:  7,
				1:  10,
				2:  4,
				3:  7,
				4:  1,
				5:  0,
				6:  4,
				7:  1,
				8:  6,
				9:  5,
				10: 2,
				11: 3,
				12: 2,
				13: 2,
				14: 2,
				15: 2,
				16: 2,
				17: 1,
				18: 2,
				19: 3,
				20: 3,
				21: 1,
				22: 1,
				23: 2,
				24: 1,
				25: 1,
				26: 1,
				27: 0,
				28: 2,
				29: 1,
				30: 1,
				31: 1,
				32: 2,
				33: 1,
				34: 1,
				35: 1,
				36: 0,
				37: 1,
				38: 1,
				39: 1,
				40: 1,
				41: 1,
				42: 1,
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

//go:build go1.18 && !functional

package sarama

import "testing"

// fuzzPanic turns any panic during decode into a test failure. Decode errors
// are an acceptable outcome for arbitrary input; panics, OOB slice access, and
// out-of-memory allocations are not.
func fuzzPanic(t *testing.T, what string, key, version int16, in []byte) {
	if r := recover(); r != nil {
		t.Fatalf("%s panic key=%d version=%d len=%d: %v\npayload=%x", what, key, version, len(in), r, in)
	}
}

// FuzzVersionedDecodeRequest exercises every supported request decoder against
// arbitrary input. A decode error is a valid outcome; a panic is not.
func FuzzVersionedDecodeRequest(f *testing.F) {
	for _, seed := range []struct {
		key, version int16
		body         []byte
	}{
		{key: apiKeyProduce, version: 0, body: produceRequestEmpty},
		{key: apiKeyProduce, version: 0, body: produceRequestHeader},
		{key: apiKeyProduce, version: 0, body: produceRequestOneMessage},
		{key: apiKeyProduce, version: 3, body: produceRequestOneRecord},
		{key: apiKeyFetch, version: 0, body: fetchRequestNoBlocks},
		{key: apiKeyFetch, version: 0, body: fetchRequestWithProperties},
		{key: apiKeyFetch, version: 0, body: fetchRequestOneBlock},
		{key: apiKeyFetch, version: 4, body: fetchRequestOneBlockV4},
		{key: apiKeyFetch, version: 11, body: fetchRequestOneBlockV11},
		{key: apiKeyFetch, version: 12, body: fetchRequestOneBlockV12},
		{key: apiKeyListOffsets, version: 1, body: offsetRequestNoBlocksV1},
		{key: apiKeyMetadata, version: 0, body: metadataRequestNoTopicsV0},
		{key: apiKeyOffsetCommit, version: 0, body: offsetCommitRequestNoBlocksV0},
		{key: apiKeyOffsetFetch, version: 0, body: offsetFetchRequestNoGroupNoPartitions},
		{key: apiKeyFindCoordinator, version: 0, body: consumerMetadataRequestEmpty},
		{key: apiKeyFindCoordinator, version: 1, body: findCoordinatorRequestConsumerGroup},
		{key: apiKeyJoinGroup, version: 0, body: joinGroupRequestV0_NoProtocols},
		{key: apiKeyHeartbeat, version: 0, body: basicHeartbeatRequestV0},
		{key: apiKeyLeaveGroup, version: 0, body: basicLeaveGroupRequestV0},
		{key: apiKeySyncGroup, version: 0, body: emptySyncGroupRequest},
		{key: apiKeyDescribeGroups, version: 0, body: emptyDescribeGroupsRequest},
		{key: apiKeyApiVersions, version: 0, body: apiVersionRequest},
		{key: apiKeyCreateTopics, version: 0, body: createTopicsRequestV0},
		{key: apiKeyDeleteTopics, version: 0, body: deleteTopicsRequest},
		{key: apiKeyInitProducerId, version: 0, body: initProducerIDRequestNull},
		{key: apiKeyTxnOffsetCommit, version: 0, body: txnOffsetCommitRequest},
		{key: apiKeyDescribeAcls, version: 0, body: aclDescribeRequest},
		{key: apiKeyCreateAcls, version: 0, body: aclCreateRequest},
		{key: apiKeyDeleteAcls, version: 1, body: aclDeleteRequestNullsv1},
		{key: apiKeyDescribeConfigs, version: 0, body: emptyDescribeConfigsRequest},
		{key: apiKeyAlterConfigs, version: 0, body: emptyAlterConfigsRequest},
		{key: apiKeyDescribeLogDirs, version: 0, body: emptyDescribeLogDirsRequest},
		{key: apiKeySASLAuth, version: 0, body: saslAuthenticateRequest},
		{key: apiKeyCreatePartitions, version: 0, body: createPartitionRequestNoAssignment},
		{key: apiKeyDeleteGroups, version: 0, body: emptyDeleteGroupsRequest},
		{key: apiKeyElectLeaders, version: 1, body: electLeadersRequestOneTopicV1},
		{key: apiKeyIncrementalAlterConfigs, version: 0, body: emptyIncrementalAlterConfigsRequest},
		{key: apiKeyAlterPartitionReassignments, version: 0, body: alterPartitionReassignmentsRequestNoBlock},
		{key: apiKeyOffsetDelete, version: 0, body: emptyDeleteOffsetsRequest},
		{key: apiKeyDescribeClientQuotas, version: 0, body: describeClientQuotasRequestAll},
		{key: apiKeyAlterClientQuotas, version: 0, body: alterClientQuotasRequestSingleOp},
		{key: apiKeyDescribeUserScramCredentials, version: 0, body: emptyDescribeUserScramCredentialsRequest},
		{key: apiKeyAlterUserScramCredentials, version: 0, body: emptyAlterUserScramCredentialsRequest},
	} {
		f.Add(seed.key, seed.version, seed.body)
	}
	f.Fuzz(func(t *testing.T, key, version int16, in []byte) {
		body := allocateBody(key, version)
		if body == nil {
			t.Skip()
		}
		defer fuzzPanic(t, "request", key, version, in)
		_ = versionedDecode(in, body, version, nil)
	})
}

// FuzzVersionedDecodeResponse exercises every supported response decoder
// against arbitrary input. A decode error is fine; a panic is not.
func FuzzVersionedDecodeResponse(f *testing.F) {
	for _, seed := range []struct {
		key, version int16
		body         []byte
	}{
		{key: apiKeyProduce, version: 0, body: produceResponseNoBlocksV0},
		{key: apiKeyFetch, version: 0, body: emptyFetchResponse},
		{key: apiKeyListOffsets, version: 0, body: emptyOffsetResponse},
		{key: apiKeyMetadata, version: 0, body: emptyMetadataResponseV0},
		{key: apiKeyOffsetCommit, version: 0, body: emptyOffsetCommitResponseV0},
		{key: apiKeyOffsetFetch, version: 0, body: offsetFetchResponseV0NullMetadata},
		{key: apiKeyFindCoordinator, version: 0, body: consumerMetadataResponseError},
		{key: apiKeyJoinGroup, version: 0, body: joinGroupResponseV0_NoError},
		{key: apiKeyHeartbeat, version: 0, body: heartbeatResponseNoError_V0},
		{key: apiKeyLeaveGroup, version: 0, body: leaveGroupResponseV0NoError},
		{key: apiKeySyncGroup, version: 0, body: syncGroupResponseV0NoError},
		{key: apiKeyDescribeGroups, version: 0, body: describeGroupsResponseEmptyV0},
		{key: apiKeyListGroups, version: 0, body: listGroupsResponseEmpty},
		{key: apiKeyApiVersions, version: 0, body: apiVersionResponseV0},
		{key: apiKeyCreateTopics, version: 0, body: createTopicsResponseV0},
		{key: apiKeyDeleteTopics, version: 0, body: deleteTopicsResponseV0},
		{key: apiKeyInitProducerId, version: 0, body: initProducerIDResponse},
		{key: apiKeyTxnOffsetCommit, version: 0, body: txnOffsetCommitResponse},
		{key: apiKeyDescribeAcls, version: 0, body: aclDescribeResponseError},
		{key: apiKeyCreateAcls, version: 0, body: createResponseWithError},
		{key: apiKeyDeleteAcls, version: 0, body: deleteAclsResponse},
		{key: apiKeyDescribeConfigs, version: 0, body: describeConfigsResponseEmpty},
		{key: apiKeyAlterConfigs, version: 0, body: alterResponseEmpty},
		{key: apiKeyDescribeLogDirs, version: 0, body: describeLogDirsResponseEmpty},
		{key: apiKeySASLAuth, version: 0, body: saslAuthenticateResponseErr},
		{key: apiKeyCreatePartitions, version: 0, body: createPartitionResponseSuccess},
		{key: apiKeyDeleteGroups, version: 0, body: emptyDeleteGroupsResponse},
		{key: apiKeyIncrementalAlterConfigs, version: 0, body: incrementalAlterResponseEmpty},
		{key: apiKeyAlterPartitionReassignments, version: 0, body: alterPartitionReassignmentsResponseNoError},
		{key: apiKeyOffsetDelete, version: 0, body: emptyDeleteOffsetsResponse},
		{key: apiKeyDescribeClientQuotas, version: 0, body: describeClientQuotasResponseError},
		{key: apiKeyAlterClientQuotas, version: 0, body: alterClientQuotasResponseError},
		{key: apiKeyDescribeUserScramCredentials, version: 0, body: emptyDescribeUserScramCredentialsResponse},
		{key: apiKeyAlterUserScramCredentials, version: 0, body: emptyAlterUserScramCredentialsResponse},
	} {
		f.Add(seed.key, seed.version, seed.body)
	}
	f.Fuzz(func(t *testing.T, key, version int16, in []byte) {
		body := allocateResponseBody(key, version)
		if body == nil {
			t.Skip()
		}
		defer fuzzPanic(t, "response", key, version, in)
		_ = versionedDecode(in, body, version, nil)
	})
}

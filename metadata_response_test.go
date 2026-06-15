//go:build !functional

package sarama

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	emptyMetadataResponseV0 = []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}

	brokersNoTopicsMetadataResponseV0 = []byte{
		0x00, 0x00, 0x00, 0x02,

		0x00, 0x00, 0xab, 0xff,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33,

		0x00, 0x01, 0x02, 0x03,
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11,

		0x00, 0x00, 0x00, 0x00,
	}

	topicsNoBrokersMetadataResponseV0 = []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x02,

		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x00,

		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, 0x00, 0x00, 0x00,
	}

	brokersNoTopicsMetadataResponseV1 = []byte{
		0x00, 0x00, 0x00, 0x02,

		0x00, 0x00, 0xab, 0xff,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33,
		0x00, 0x05, 'r', 'a', 'c', 'k', '0',

		0x00, 0x01, 0x02, 0x03,
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11,
		0x00, 0x05, 'r', 'a', 'c', 'k', '1',

		0x00, 0x00, 0x00, 0x01,

		0x00, 0x00, 0x00, 0x00,
	}

	topicsNoBrokersMetadataResponseV1 = []byte{
		0x00, 0x00, 0x00, 0x00,

		0x00, 0x00, 0x00, 0x04,

		0x00, 0x00, 0x00, 0x02,

		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x00,

		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x01,
		0x00, 0x00, 0x00, 0x00,
	}

	noBrokersNoTopicsWithThrottleTimeAndClusterIDV3 = []byte{
		0x00, 0x00, 0x00, 0x10,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x09, 'c', 'l', 'u', 's', 't', 'e', 'r', 'I', 'd',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
	}

	noBrokersOneTopicWithOfflineReplicasV5 = []byte{
		0x00, 0x00, 0x00, 0x05,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x09, 'c', 'l', 'u', 's', 't', 'e', 'r', 'I', 'd',
		0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03,
	}

	OneTopicV6 = []byte{
		0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 'h', 'o', 's',
		't', 0x00, 0x00, 0x23, 0x84, 0xff, 0xff, 0x00, 0x09, 'c', 'l', 'u', 's', 't', 'e', 'r',
		'I', 'd', 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 't', 'o',
		'n', 'y', 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00, 0x00,
	}

	OneTopicV7 = []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 'h', 'o', 's',
		't', 0x00, 0x00, 0x23, 0x84, 0xff, 0xff, 0x00, 0x09, 'c', 'l', 'u', 's', 't', 'e', 'r',
		'I', 'd', 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 't', 'o',
		'n', 'y', 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x7b, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
	}

	OneTopicV8 = []byte{
		0x00, 0x00, 0x00, 0x00, // throttle ms
		0x00, 0x00, 0x00, 0x01, // length brokers
		0x00, 0x00, 0x00, 0x00, // broker[0].nodeid
		0x00, 0x04, // brokers[0].length(nodehost)
		'h', 'o', 's', 't', // broker[0].nodehost
		0x00, 0x00, 0x23, 0x84, // broker[0].port (9092)
		0xff, 0xff, // brokers[0].rack (null)
		0x00, 0x09, 'c', 'l', 'u', 's', 't', 'e', 'r',
		'I', 'd', 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 't', 'o',
		'n', 'y', 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x7b, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 'Y', 0x00, 0x00, 0x00,
		0xea,
	}

	OneTopicV9 = []byte{
		0x00, 0x00, 0x00, 0x00, // throttle ms
		0x02,                   // length of brokers
		0x00, 0x00, 0x00, 0x00, // broker[0].nodeid
		0x05,               // length of brokers[0].nodehost
		'h', 'o', 's', 't', // brokers[0].nodehost
		0x00, 0x00, 0x23, 0x84, // brokers[0].port (9092)
		0x00,                                              // brokers[0].rack (null)
		0x00,                                              // empty tags
		0x0a, 'c', 'l', 'u', 's', 't', 'e', 'r', 'I', 'd', // cluster id
		0x00, 0x00, 0x00,
		0x01, 0x02, 0x00, 0x00, 0x05, 't', 'o', 'n', 'y', 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7b, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
		0x00, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x01, 'Y', 0x00, 0x00, 0x00, 0x00, 0xea, 0x00,
	}

	OneTopicV10 = []byte{
		0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x05, 'h', 'o', 's', 't', 0x00, 0x00, 0x23,
		0x84, 0x00, 0x00, 0x0a, 'c', 'l', 'u', 's', 't', 'e', 'r', 'I', 'd', 0x00, 0x00, 0x00,
		0x01, 0x02, 0x00, 0x00, 0x05, 't', 'o', 'n', 'y', 0x84, 0xcd, 0xa7, 'U', 0x7e, 0x84, 'K',
		0xf9, 0xb7, 0xdc, 0xfc, 0x11, 0x82, 0x07, 'r', 'J', 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7b, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
		0x00, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x01, 'Y', 0x00, 0x00, 0x00, 0x00, 0xea, 0x00,
	}

	// v11 drops the ClusterAuthorizedOperations response field (KIP-700);
	// it is now exposed by the DescribeCluster API.
	OneTopicV11 = []byte{
		0x00, 0x00, 0x00, 0x00, // ThrottleTimeMs
		0x02, // Brokers
		// broker
		0x00, 0x00, 0x00, 0x00, // NodeId
		0x05, 'h', 'o', 's', 't', // Host
		0x00, 0x00, 0x23, 0x84, // Port
		0x00, // Rack
		0x00, // tagged fields

		0x0a, 'c', 'l', 'u', 's', 't', 'e', 'r', 'I', 'd', // ClusterId
		0x00, 0x00, 0x00, 0x01, // ControllerId
		0x02, // Topics
		// topic
		0x00, 0x00, // ErrorCode
		0x05, 't', 'o', 'n', 'y', // Name
		0x84, 0xcd, 0xa7, 'U', 0x7e, 0x84, 'K', 0xf9, 0xb7, 0xdc, 0xfc, 0x11, 0x82, 0x07, 'r', 'J', // TopicId
		0x00, // IsInternal
		0x02, // Partitions
		// partition
		0x00, 0x00, // ErrorCode
		0x00, 0x00, 0x00, 0x00, // PartitionIndex
		0x00, 0x00, 0x00, 0x00, // LeaderId
		0x00, 0x00, 0x00, 0x7b, // LeaderEpoch
		0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, // ReplicaNodes
		0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, // IsrNodes
		0x01, // OfflineReplicas
		0x00, // tagged fields

		0x00, 0x00, 0x01, 'Y', // TopicAuthorizedOperations
		0x00, // tagged fields

		0x00, // tagged fields
	}
)

func TestEmptyMetadataResponseV0(t *testing.T) {
	response := MetadataResponse{}

	testVersionDecodable(t, "empty, V0", &response, emptyMetadataResponseV0, 0)
	if len(response.Brokers) != 0 {
		t.Error("Decoding produced", len(response.Brokers), "brokers where there were none!")
	}
	if len(response.Topics) != 0 {
		t.Error("Decoding produced", len(response.Topics), "topics where there were none!")
	}
}

func TestMetadataResponseWithBrokersV0(t *testing.T) {
	response := MetadataResponse{}

	testVersionDecodable(t, "brokers, no topics, V0", &response, brokersNoTopicsMetadataResponseV0, 0)
	if len(response.Brokers) != 2 {
		t.Fatal("Decoding produced", len(response.Brokers), "brokers where there were two!")
	}

	if response.Brokers[0].id != 0xabff {
		t.Error("Decoding produced invalid broker 0 id.")
	}
	if response.Brokers[0].addr != "localhost:51" {
		t.Error("Decoding produced invalid broker 0 address.")
	}
	if response.Brokers[1].id != 0x010203 {
		t.Error("Decoding produced invalid broker 1 id.")
	}
	if response.Brokers[1].addr != "google.com:273" {
		t.Error("Decoding produced invalid broker 1 address.")
	}

	if len(response.Topics) != 0 {
		t.Error("Decoding produced", len(response.Topics), "topics where there were none!")
	}
}

func TestMetadataResponseWithTopicsV0(t *testing.T) {
	response := MetadataResponse{}

	testVersionDecodable(t, "topics, no brokers, V0", &response, topicsNoBrokersMetadataResponseV0, 0)
	if len(response.Brokers) != 0 {
		t.Error("Decoding produced", len(response.Brokers), "brokers where there were none!")
	}

	if len(response.Topics) != 2 {
		t.Fatal("Decoding produced", len(response.Topics), "topics where there were two!")
	}

	if !errors.Is(response.Topics[0].Err, ErrNoError) {
		t.Error("Decoding produced invalid topic 0 error.")
	}

	if response.Topics[0].Name != "foo" {
		t.Error("Decoding produced invalid topic 0 name.")
	}

	if len(response.Topics[0].Partitions) != 1 {
		t.Fatal("Decoding produced invalid partition count for topic 0.")
	}

	if !errors.Is(response.Topics[0].Partitions[0].Err, ErrInvalidMessageSize) {
		t.Error("Decoding produced invalid topic 0 partition 0 error.")
	}

	if response.Topics[0].Partitions[0].ID != 0x01 {
		t.Error("Decoding produced invalid topic 0 partition 0 id.")
	}

	if response.Topics[0].Partitions[0].Leader != 0x07 {
		t.Error("Decoding produced invalid topic 0 partition 0 leader.")
	}

	if len(response.Topics[0].Partitions[0].Replicas) != 3 {
		t.Fatal("Decoding produced invalid topic 0 partition 0 replicas.")
	}
	for i := range 3 {
		if response.Topics[0].Partitions[0].Replicas[i] != int32(i+1) {
			t.Error("Decoding produced invalid topic 0 partition 0 replica", i)
		}
	}

	if len(response.Topics[0].Partitions[0].Isr) != 0 {
		t.Error("Decoding produced invalid topic 0 partition 0 isr length.")
	}

	if !errors.Is(response.Topics[1].Err, ErrNoError) {
		t.Error("Decoding produced invalid topic 1 error.")
	}

	if response.Topics[1].Name != "bar" {
		t.Error("Decoding produced invalid topic 0 name.")
	}

	if len(response.Topics[1].Partitions) != 0 {
		t.Error("Decoding produced invalid partition count for topic 1.")
	}
}

func TestMetadataResponseWithBrokersV1(t *testing.T) {
	response := MetadataResponse{}

	testVersionDecodable(t, "topics, V1", &response, brokersNoTopicsMetadataResponseV1, 1)
	if len(response.Brokers) != 2 {
		t.Error("Decoding produced", len(response.Brokers), "brokers where there were 2!")
	}
	if response.Brokers[0].rack == nil || *response.Brokers[0].rack != "rack0" {
		t.Error("Decoding produced invalid broker 0 rack.")
	}
	if response.Brokers[1].rack == nil || *response.Brokers[1].rack != "rack1" {
		t.Error("Decoding produced invalid broker 1 rack.")
	}
	if response.ControllerID != 1 {
		t.Error("Decoding produced", response.ControllerID, "should have been 1!")
	}
	if len(response.Topics) != 0 {
		t.Error("Decoding produced", len(response.Brokers), "brokers where there were none!")
	}
}

func TestMetadataResponseWithTopicsV1(t *testing.T) {
	response := MetadataResponse{}

	testVersionDecodable(t, "topics, V1", &response, topicsNoBrokersMetadataResponseV1, 1)
	if len(response.Brokers) != 0 {
		t.Error("Decoding produced", len(response.Brokers), "brokers where there were none!")
	}
	if response.ControllerID != 4 {
		t.Error("Decoding produced", response.ControllerID, "should have been 4!")
	}
	if len(response.Topics) != 2 {
		t.Error("Decoding produced", len(response.Topics), "topics where there were 2!")
	}
	if response.Topics[0].IsInternal {
		t.Error("Decoding produced", response.Topics[0], "topic0 should have been false!")
	}
	if !response.Topics[1].IsInternal {
		t.Error("Decoding produced", response.Topics[1], "topic1 should have been true!")
	}
}

func TestMetadataResponseWithThrottleTime(t *testing.T) {
	response := MetadataResponse{}

	testVersionDecodable(t, "no topics, no brokers, throttle time and cluster Id V3", &response, noBrokersNoTopicsWithThrottleTimeAndClusterIDV3, 3)
	if response.ThrottleTimeMs != int32(16) {
		t.Error("Decoding produced", response.ThrottleTimeMs, "should have been 16!")
	}
	if len(response.Brokers) != 0 {
		t.Error("Decoding produced", response.Brokers, "should have been 0!")
	}
	if response.ControllerID != int32(1) {
		t.Error("Decoding produced", response.ControllerID, "should have been 1!")
	}
	if *response.ClusterID != "clusterId" {
		t.Error("Decoding produced", response.ClusterID, "should have been clusterId!")
	}
	if len(response.Topics) != 0 {
		t.Error("Decoding produced", len(response.Topics), "should have been 0!")
	}
}

func TestMetadataResponseWithOfflineReplicasV5(t *testing.T) {
	response := MetadataResponse{}

	testVersionDecodable(t, "no brokers, 1 topic with offline replica V5", &response, noBrokersOneTopicWithOfflineReplicasV5, 5)
	if response.ThrottleTimeMs != int32(5) {
		t.Error("Decoding produced", response.ThrottleTimeMs, "should have been 5!")
	}
	if len(response.Brokers) != 0 {
		t.Error("Decoding produced", response.Brokers, "should have been 0!")
	}
	if response.ControllerID != int32(2) {
		t.Error("Decoding produced", response.ControllerID, "should have been 21!")
	}
	if *response.ClusterID != "clusterId" {
		t.Error("Decoding produced", response.ClusterID, "should have been clusterId!")
	}
	if len(response.Topics) != 1 {
		t.Error("Decoding produced", len(response.Topics), "should have been 1!")
	}
	if len(response.Topics[0].Partitions[0].OfflineReplicas) != 1 {
		t.Error("Decoding produced", len(response.Topics[0].Partitions[0].OfflineReplicas), "should have been 1!")
	}
}

func TestMetadataResponseV6(t *testing.T) {
	response := MetadataResponse{}

	testVersionDecodable(t, "no brokers, 1 topic with offline replica V5", &response, OneTopicV6, 6)
	if response.ThrottleTimeMs != int32(7) {
		t.Error("Decoding produced", response.ThrottleTimeMs, "should have been 7!")
	}
	if len(response.Brokers) != 1 {
		t.Error("Decoding produced", response.Brokers, "should have been 1!")
	}
	if response.Brokers[0].addr != "host:9092" {
		t.Error("Decoding produced", response.Brokers[0].addr, "should have been host:9092!")
	}
	if response.ControllerID != int32(1) {
		t.Error("Decoding produced", response.ControllerID, "should have been 1!")
	}
	if *response.ClusterID != "clusterId" {
		t.Error("Decoding produced", response.ClusterID, "should have been clusterId!")
	}
	if len(response.Topics) != 1 {
		t.Error("Decoding produced", len(response.Topics), "should have been 1!")
	}
	if len(response.Topics[0].Partitions[0].OfflineReplicas) != 0 {
		t.Error("Decoding produced", len(response.Topics[0].Partitions[0].OfflineReplicas), "should have been 0!")
	}
}

func TestMetadataResponseV7(t *testing.T) {
	response := MetadataResponse{}

	testVersionDecodable(t, "no brokers, 1 topic with offline replica V5", &response, OneTopicV7, 7)
	if response.ThrottleTimeMs != int32(0) {
		t.Error("Decoding produced", response.ThrottleTimeMs, "should have been 0!")
	}
	if len(response.Brokers) != 1 {
		t.Error("Decoding produced", response.Brokers, "should have been 1!")
	}
	if response.Brokers[0].addr != "host:9092" {
		t.Error("Decoding produced", response.Brokers[0].addr, "should have been host:9092!")
	}
	if response.ControllerID != int32(1) {
		t.Error("Decoding produced", response.ControllerID, "should have been 1!")
	}
	if *response.ClusterID != "clusterId" {
		t.Error("Decoding produced", response.ClusterID, "should have been clusterId!")
	}
	if len(response.Topics) != 1 {
		t.Error("Decoding produced", len(response.Topics), "should have been 1!")
	}
	if len(response.Topics[0].Partitions[0].OfflineReplicas) != 0 {
		t.Error("Decoding produced", len(response.Topics[0].Partitions[0].OfflineReplicas), "should have been 0!")
	}
	if response.Topics[0].Partitions[0].LeaderEpoch != 123 {
		t.Error("Decoding produced", response.Topics[0].Partitions[0].LeaderEpoch, "should have been 123!")
	}
}

func TestMetadataResponseV8(t *testing.T) {
	response := MetadataResponse{}

	testVersionDecodable(t, "no brokers, 1 topic with offline replica V5", &response, OneTopicV8, 8)
	if response.ThrottleTimeMs != int32(0) {
		t.Error("Decoding produced", response.ThrottleTimeMs, "should have been 0!")
	}
	if len(response.Brokers) != 1 {
		t.Error("Decoding produced", response.Brokers, "should have been 1!")
	}
	if response.Brokers[0].addr != "host:9092" {
		t.Error("Decoding produced", response.Brokers[0].addr, "should have been host:9092!")
	}
	if response.ControllerID != int32(1) {
		t.Error("Decoding produced", response.ControllerID, "should have been 1!")
	}
	if *response.ClusterID != "clusterId" {
		t.Error("Decoding produced", response.ClusterID, "should have been clusterId!")
	}
	if response.ClusterAuthorizedOperations != 234 {
		t.Error("Decoding produced", response.ClusterAuthorizedOperations, "should have been 234!")
	}
	if len(response.Topics) != 1 {
		t.Error("Decoding produced", len(response.Topics), "should have been 1!")
	}
	if response.Topics[0].TopicAuthorizedOperations != 345 {
		t.Error("Decoding produced", response.Topics[0].TopicAuthorizedOperations, "should have been 345!")
	}
	if len(response.Topics[0].Partitions[0].OfflineReplicas) != 0 {
		t.Error("Decoding produced", len(response.Topics[0].Partitions[0].OfflineReplicas), "should have been 0!")
	}
	if response.Topics[0].Partitions[0].LeaderEpoch != 123 {
		t.Error("Decoding produced", response.Topics[0].Partitions[0].LeaderEpoch, "should have been 123!")
	}
}

func TestMetadataResponseV9(t *testing.T) {
	response := MetadataResponse{}

	testVersionDecodable(t, "no brokers, 1 topic with offline replica V5", &response, OneTopicV9, 9)
	if response.ThrottleTimeMs != int32(0) {
		t.Error("Decoding produced", response.ThrottleTimeMs, "should have been 0!")
	}
	if len(response.Brokers) != 1 {
		t.Error("Decoding produced", response.Brokers, "should have been 1!")
	}
	if response.Brokers[0].addr != "host:9092" {
		t.Error("Decoding produced", response.Brokers[0].addr, "should have been host:9092!")
	}
	if response.ControllerID != int32(1) {
		t.Error("Decoding produced", response.ControllerID, "should have been 1!")
	}
	if *response.ClusterID != "clusterId" {
		t.Error("Decoding produced", response.ClusterID, "should have been clusterId!")
	}
	if response.ClusterAuthorizedOperations != 234 {
		t.Error("Decoding produced", response.ClusterAuthorizedOperations, "should have been 234!")
	}
	if len(response.Topics) != 1 {
		t.Error("Decoding produced", len(response.Topics), "should have been 1!")
	}
	if response.Topics[0].TopicAuthorizedOperations != 345 {
		t.Error("Decoding produced", response.Topics[0].TopicAuthorizedOperations, "should have been 345!")
	}
	if len(response.Topics[0].Partitions[0].OfflineReplicas) != 0 {
		t.Error("Decoding produced", len(response.Topics[0].Partitions[0].OfflineReplicas), "should have been 0!")
	}
	if response.Topics[0].Partitions[0].LeaderEpoch != 123 {
		t.Error("Decoding produced", response.Topics[0].Partitions[0].LeaderEpoch, "should have been 123!")
	}
}

func TestMetadataResponseV10(t *testing.T) {
	response := MetadataResponse{}

	testVersionDecodable(t, "no brokers, 1 topic with offline replica V5", &response, OneTopicV10, 10)
	if response.ThrottleTimeMs != int32(0) {
		t.Error("Decoding produced", response.ThrottleTimeMs, "should have been 0!")
	}
	if len(response.Brokers) != 1 {
		t.Error("Decoding produced", response.Brokers, "should have been 1!")
	}
	if response.Brokers[0].addr != "host:9092" {
		t.Error("Decoding produced", response.Brokers[0].addr, "should have been host:9092!")
	}
	if response.ControllerID != int32(1) {
		t.Error("Decoding produced", response.ControllerID, "should have been 1!")
	}
	if *response.ClusterID != "clusterId" {
		t.Error("Decoding produced", response.ClusterID, "should have been clusterId!")
	}
	if response.ClusterAuthorizedOperations != 234 {
		t.Error("Decoding produced", response.ClusterAuthorizedOperations, "should have been 234!")
	}
	if len(response.Topics) != 1 {
		t.Error("Decoding produced", len(response.Topics), "should have been 1!")
	}
	if response.Topics[0].Uuid != [16]byte{
		0x84, 0xcd, 0xa7, 0x55, 0x7e, 0x84, 0x4b, 0xf9,
		0xb7, 0xdc, 0xfc, 0x11, 0x82, 0x07, 0x72, 0x4a,
	} {
		t.Error("Decoding produced", response.Topics[0].Uuid, "should have been different!")
	}
	if response.Topics[0].TopicAuthorizedOperations != 345 {
		t.Error("Decoding produced", response.Topics[0].TopicAuthorizedOperations, "should have been 345!")
	}
	if len(response.Topics[0].Partitions[0].OfflineReplicas) != 0 {
		t.Error("Decoding produced", len(response.Topics[0].Partitions[0].OfflineReplicas), "should have been 0!")
	}
	if response.Topics[0].Partitions[0].LeaderEpoch != 123 {
		t.Error("Decoding produced", response.Topics[0].Partitions[0].LeaderEpoch, "should have been 123!")
	}
}

func TestMetadataResponseV11(t *testing.T) {
	response := MetadataResponse{}

	testVersionDecodable(t, "1 topic with topic id V11", &response, OneTopicV11, 11)
	assert.Zero(t, response.ThrottleTimeMs)
	require.Len(t, response.Brokers, 1)
	assert.Equal(t, "host:9092", response.Brokers[0].addr)
	assert.Equal(t, int32(1), response.ControllerID)
	require.NotNil(t, response.ClusterID)
	assert.Equal(t, "clusterId", *response.ClusterID)
	require.Len(t, response.Topics, 1)
	assert.Equal(t, Uuid{
		0x84, 0xcd, 0xa7, 0x55, 0x7e, 0x84, 0x4b, 0xf9,
		0xb7, 0xdc, 0xfc, 0x11, 0x82, 0x07, 0x72, 0x4a,
	}, response.Topics[0].Uuid)
	assert.Equal(t, int32(345), response.Topics[0].TopicAuthorizedOperations)
	require.Len(t, response.Topics[0].Partitions, 1)
	assert.Empty(t, response.Topics[0].Partitions[0].OfflineReplicas)
	assert.Equal(t, int32(123), response.Topics[0].Partitions[0].LeaderEpoch)
}

func TestFlexibleMetadataResponseInvalidInt32ArrayLength(t *testing.T) {
	packet := []byte{
		0x00, 0x00, 0x00, 0x00, // throttle ms
		0x01,                   // length of brokers (0)
		0x00,                   // null cluster id
		0x00, 0x00, 0x00, 0x00, // controller id
		0x02,       // length of topics (1)
		0x00, 0x00, // topic error
		0x02, 't', // topic name
		0x00,       // is internal
		0x02,       // length of partitions (1)
		0x00, 0x00, // partition error
		0x00, 0x00, 0x00, 0x00, // partition id
		0x00, 0x00, 0x00, 0x00, // leader id
		0x00, 0x00, 0x00, 0x00, // leader epoch
		0x2, // length of compact array (1) should trigger ErrInsufficientData
	}

	response := MetadataResponse{}
	err := versionedDecode(packet, &response, 9, nil)
	if err == nil {
		t.Error("expected an error decoding a truncated flexible int32 array, got nil")
	}
}

func TestFlexibleMetadataResponseInvalidInt32ArrayLengthOverflow(t *testing.T) {
	packet := []byte{
		0x00, 0x00, 0x00, 0x00, // throttle ms
		0x01,                   // length of brokers (0)
		0x00,                   // null cluster id
		0x00, 0x00, 0x00, 0x00, // controller id
		0x02,       // length of topics (1)
		0x00, 0x00, // topic error
		0x02, 't', // topic name
		0x00,       // is internal
		0x02,       // length of partitions (1)
		0x00, 0x00, // partition error
		0x00, 0x00, 0x00, 0x00, // partition id
		0x00, 0x00, 0x00, 0x00, // leader id
		0x00, 0x00, 0x00, 0x00, // leader epoch
		0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x21, // length
	}

	response := MetadataResponse{}
	err := versionedDecode(packet, &response, 9, nil)
	if err == nil {
		t.Error("expected an error decoding a truncated flexible int32 array, got nil")
	}
}

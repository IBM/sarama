//go:build functional

package sarama

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func topicWithEvenLeaders(t *testing.T, adminClient ClusterAdmin, client Client, numPartitions int32) (string, error) {
	t.Helper()

	if len(FunctionalTestEnv.KafkaBrokerAddrs) == 0 {
		return "", fmt.Errorf("no brokers available for replica assignment")
	}

	brokers := client.Brokers()
	brokerIDs := make([]int32, 0, len(brokers))
	for _, broker := range brokers {
		brokerIDs = append(brokerIDs, broker.ID())
	}
	if len(brokerIDs) == 0 {
		return "", fmt.Errorf("no broker IDs available for replica assignment")
	}
	slices.Sort(brokerIDs)

	topic := fmt.Sprintf("list-offsets-%d", time.Now().UnixNano())
	replicaAssignment := make(map[int32][]int32, numPartitions)
	for partition := int32(0); partition < numPartitions; partition++ {
		brokerIndex := partition % int32(len(brokerIDs))
		replicaAssignment[partition] = []int32{brokerIDs[brokerIndex]}
	}

	err := adminClient.CreateTopic(topic, &TopicDetail{
		NumPartitions:     -1,
		ReplicationFactor: -1,
		ReplicaAssignment: replicaAssignment,
	}, false)
	if err != nil {
		return "", err
	}

	// topic creation is asynchronous; wait for every partition to have an
	// elected leader before returning (producing too early fails with
	// ErrNotLeaderForPartition or ErrUnknownTopicOrPartition)
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		require.NoError(t, client.RefreshMetadata(topic))
		for partition := int32(0); partition < numPartitions; partition++ {
			leader, err := client.Leader(topic, partition)
			require.NoError(t, err, "no leader for %s/%d", topic, partition)
			require.NotNil(t, leader)
		}
	}, 30*time.Second, 250*time.Millisecond, "leaders were not elected for all partitions of %s", topic)

	return topic, nil
}

func produceMessagesForPartitions(t *testing.T, client Client, topic string, partitionsCount int32, messagesPerPartition int, baseTimestamp int64) {
	t.Helper()

	producer, err := NewSyncProducerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, producer)

	for partition := int32(0); partition < partitionsCount; partition++ {
		for msgIndex := 0; msgIndex < messagesPerPartition; msgIndex++ {
			ts := baseTimestamp + int64(msgIndex)*1000
			value := fmt.Sprintf("p%d-v%d", partition, msgIndex+1)
			message := &ProducerMessage{
				Topic:     topic,
				Partition: partition,
				Timestamp: time.UnixMilli(ts),
				Value:     StringEncoder(value),
			}
			if _, _, err := producer.SendMessage(message); err != nil {
				t.Fatalf("produce message partition=%d index=%d: %v", partition, msgIndex, err)
			}
		}
	}
}

func listOffsetsAndValidate(
	t *testing.T,
	adminClient ClusterAdmin,
	topic string,
	partitionsCount int32,
	offsetQuery int64,
	expectedOffset int64,
	label string,
) {
	t.Helper()

	offsetQueries := make(map[string]map[int32]int64, 1)
	offsetQueries[topic] = make(map[int32]int64, partitionsCount)
	for partition := int32(0); partition < partitionsCount; partition++ {
		offsetQueries[topic][partition] = offsetQuery
	}

	results, err := adminClient.ListOffsets(offsetQueries, nil)
	if err != nil {
		t.Fatal(err)
	}

	for partition := int32(0); partition < partitionsCount; partition++ {
		result := results[topic][partition]
		if result == nil {
			t.Fatalf("missing %s result for %s/%d", label, topic, partition)
		}
		if !errors.Is(result.Err, ErrNoError) {
			t.Fatalf("unexpected %s error for %s/%d: %v", label, topic, partition, result.Err)
		}
		if result.Offset != expectedOffset {
			t.Fatalf(
				"unexpected %s offset for %s/%d: want %d, got %d",
				label,
				topic,
				partition,
				expectedOffset,
				result.Offset,
			)
		}
	}
}

func TestFuncAdminQuotas(t *testing.T) {
	t.Parallel()
	const (
		waitFor = 10 * time.Second
		tick    = 100 * time.Millisecond
	)
	checkKafkaVersion(t, "2.6.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	kafkaVersion, err := ParseKafkaVersion(FunctionalTestEnv.KafkaVersion)
	if err != nil {
		t.Fatal(err)
	}

	config := NewFunctionalTestConfig()
	config.Version = kafkaVersion
	adminClient, err := NewClusterAdmin(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, adminClient)

	// Check that we can read the quotas, and that they are empty
	quotas, err := adminClient.DescribeClientQuotas(nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(quotas) != 0 {
		t.Fatalf("Expected quotas to be empty at start, found: %v", quotas)
	}

	// Put a quota on default user
	// /config/users/<default>
	defaultUser := []QuotaEntityComponent{{
		EntityType: QuotaEntityUser,
		MatchType:  QuotaMatchDefault,
	}}
	produceOp := ClientQuotasOp{
		Key:   "producer_byte_rate",
		Value: 1024000,
	}
	if err = adminClient.AlterClientQuotas(defaultUser, produceOp, false); err != nil {
		t.Fatal(err)
	}

	// Poll until we have the expected quota entry
	defaultUserFilter := QuotaFilterComponent{
		EntityType: QuotaEntityUser,
		MatchType:  QuotaMatchDefault,
	}
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		quotas, err = adminClient.DescribeClientQuotas(
			[]QuotaFilterComponent{defaultUserFilter},
			false,
		)
		require.NoError(t, err)
		require.NotEmpty(t, quotas, "Expected not empty quotas for default user")
		require.Len(t, quotas, 1, "Expected one quota entry for default user")
	}, waitFor, tick, "Quotas state has still not updated for default user")

	// Put a quota on specific client-id for a specific user
	// /config/users/<user>/clients/<client-id>
	specificUserClientID := []QuotaEntityComponent{
		{
			EntityType: QuotaEntityUser,
			MatchType:  QuotaMatchExact,
			Name:       "sarama",
		},
		{
			EntityType: QuotaEntityClientID,
			MatchType:  QuotaMatchExact,
			Name:       "sarama-consumer",
		},
	}
	consumeOp := ClientQuotasOp{
		Key:   "consumer_byte_rate",
		Value: 2048000,
	}
	if err = adminClient.AlterClientQuotas(specificUserClientID, consumeOp, false); err != nil {
		t.Fatal(err)
	}

	// Check that we can query a specific quota entry
	userFilter := QuotaFilterComponent{
		EntityType: QuotaEntityUser,
		MatchType:  QuotaMatchExact,
		Match:      "sarama",
	}
	clientFilter := QuotaFilterComponent{
		EntityType: QuotaEntityClientID,
		MatchType:  QuotaMatchExact,
		Match:      "sarama-consumer",
	}
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		quotas, err = adminClient.DescribeClientQuotas(
			[]QuotaFilterComponent{userFilter, clientFilter},
			true,
		)
		require.NoError(t, err)
		require.NotEmpty(t, quotas, "Expected not empty quotas for specific clientID")
		require.Len(t, quotas, 1, "Expected one quota entry for specific clientID")
		require.InDelta(
			t,
			quotas[0].Values[consumeOp.Key],
			consumeOp.Value,
			0.01,
			"Expected specific quota value to be %f, found: %v",
			consumeOp.Value,
			quotas[0].Values[consumeOp.Key],
		)
	}, waitFor, tick, "Quotas state for specific clientID has still not updated")

	// Remove quota entries
	deleteProduceOp := ClientQuotasOp{
		Key:    produceOp.Key,
		Remove: true,
	}
	if err = adminClient.AlterClientQuotas(defaultUser, deleteProduceOp, false); err != nil {
		t.Fatal(err)
	}

	deleteConsumeOp := ClientQuotasOp{
		Key:    consumeOp.Key,
		Remove: true,
	}
	if err = adminClient.AlterClientQuotas(specificUserClientID, deleteConsumeOp, false); err != nil {
		t.Fatal(err)
	}
}

func TestFuncAdminDescribeGroups(t *testing.T) {
	t.Parallel()
	checkKafkaVersion(t, "2.3.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	group1 := testFuncConsumerGroupID(t)
	group2 := testFuncConsumerGroupID(t)

	kafkaVersion, err := ParseKafkaVersion(FunctionalTestEnv.KafkaVersion)
	if err != nil {
		t.Fatal(err)
	}

	config := NewFunctionalTestConfig()
	config.Version = kafkaVersion
	adminClient, err := NewClusterAdmin(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, adminClient)

	config1 := NewFunctionalTestConfig()
	config1.ClientID = "M1"
	config1.Version = V2_3_0_0
	config1.Consumer.Offsets.Initial = OffsetNewest
	m1 := runTestFuncConsumerGroupMemberWithConfig(t, config1, group1, 100, nil, "test.4")
	defer m1.Close()

	config2 := NewFunctionalTestConfig()
	config2.ClientID = "M2"
	config2.Version = V2_3_0_0
	config2.Consumer.Offsets.Initial = OffsetNewest
	config2.Consumer.Group.InstanceId = "Instance2"
	m2 := runTestFuncConsumerGroupMemberWithConfig(t, config2, group2, 100, nil, "test.4")
	defer m2.Close()

	m1.WaitForState(2)
	m2.WaitForState(2)

	res, err := adminClient.DescribeConsumerGroups([]string{group1, group2})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Errorf("group description should be 2, got %v\n", len(res))
	}
	if len(res[0].Members) != 1 {
		t.Errorf("should have 1 members in group , got %v\n", len(res[0].Members))
	}
	if len(res[1].Members) != 1 {
		t.Errorf("should have 1 members in group , got %v\n", len(res[1].Members))
	}

	m1.AssertCleanShutdown()
	m2.AssertCleanShutdown()
}

func TestFuncAdminListConsumerGroups(t *testing.T) {
	t.Parallel()
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	group1 := testFuncConsumerGroupID(t)
	group2 := testFuncConsumerGroupID(t)

	config := NewFunctionalTestConfig()
	adminClient, err := NewClusterAdmin(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, adminClient)

	config1 := NewFunctionalTestConfig()
	config1.ClientID = "M1"
	config1.Consumer.Offsets.Initial = OffsetNewest
	m1 := runTestFuncConsumerGroupMemberWithConfig(t, config1, group1, 100, nil, "test.4")
	defer m1.Close()

	config2 := NewFunctionalTestConfig()
	config2.ClientID = "M2"
	config2.Consumer.Offsets.Initial = OffsetNewest
	config2.Consumer.Group.InstanceId = "Instance2"
	m2 := runTestFuncConsumerGroupMemberWithConfig(t, config2, group2, 100, nil, "test.4")
	defer m2.Close()

	m1.WaitForState(2)
	m2.WaitForState(2)

	res, err := adminClient.ListConsumerGroups()
	if err != nil {
		t.Fatal(err)
	}
	assert.GreaterOrEqual(t, len(res), 2)
	assert.Contains(t, slices.Collect(maps.Keys(res)), group1)
	assert.Contains(t, slices.Collect(maps.Keys(res)), group2)

	m1.AssertCleanShutdown()
	m2.AssertCleanShutdown()
}

func TestFuncAdminListConsumerGroupOffsets(t *testing.T) {
	checkKafkaVersion(t, "0.8.2.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.ClientID = t.Name()
	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	defer safeClose(t, client)
	if err != nil {
		t.Fatal(err)
	}

	group := testFuncConsumerGroupID(t)
	consumerGroup, err := NewConsumerGroupFromClient(group, client)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, consumerGroup)

	offsetMgr, _ := NewOffsetManagerFromClient(group, client)
	defer safeClose(t, offsetMgr)
	markOffset(t, offsetMgr, "test.4", 0, 2)
	offsetMgr.Commit()

	coordinator, err := client.Coordinator(group)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("coordinator broker %d", coordinator.id)

	adminClient, err := NewClusterAdminFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	{
		resp, err := adminClient.ListConsumerGroupOffsets(group, map[string][]int32{"test.4": {0, 1, 2, 3}})
		if err != nil {
			t.Fatal(err)
		}
		t.Log(spew.Sdump(resp))
	}

	brokerID := coordinator.id
	t.Cleanup(
		func() {
			if err := startDockerTestBroker(context.Background(), brokerID); err != nil {
				t.Fatal(err)
			}
		},
	)
	if err := stopDockerTestBroker(context.Background(), brokerID); err != nil {
		t.Fatal(err)
	}

	{
		resp, err := adminClient.ListConsumerGroupOffsets(group, map[string][]int32{"test.4": {0, 1, 2, 3}})
		if err != nil {
			t.Fatal(err)
		}
		t.Log(spew.Sdump(resp))
	}

	coordinator, err = adminClient.Coordinator(group)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("coordinator broker %d", coordinator.id)
}

func TestFuncAdminListConsumerGroupOffsetsBatch(t *testing.T) {
	checkKafkaVersion(t, "3.0.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.ClientID = t.Name()
	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer safeClose(t, client)

	groupA := testFuncConsumerGroupID(t)
	groupB := testFuncConsumerGroupID(t)
	const (
		topic      = "test.4"
		partition  = int32(0)
		offsetGrpA = int64(2)
		offsetGrpB = int64(3)
	)

	for _, c := range []struct {
		group  string
		offset int64
	}{{groupA, offsetGrpA}, {groupB, offsetGrpB}} {
		offsetMgr, err := NewOffsetManagerFromClient(c.group, client)
		require.NoError(t, err)
		markOffset(t, offsetMgr, topic, partition, c.offset)
		offsetMgr.Commit()
		safeClose(t, offsetMgr)
	}

	adminClient, err := NewClusterAdminFromClient(client)
	require.NoError(t, err)

	result, err := adminClient.ListConsumerGroupOffsetsBatch(map[string]map[string][]int32{
		groupA: {topic: {partition}},
		groupB: {topic: {partition}},
	})
	require.NoError(t, err)

	for _, c := range []struct {
		group  string
		offset int64
	}{{groupA, offsetGrpA}, {groupB, offsetGrpB}} {
		require.Contains(t, result, c.group)
		require.Equal(t, ErrNoError, result[c.group].Err)
		block := result[c.group].GetBlock(topic, partition)
		require.NotNil(t, block, "missing block for %s/%s/%d", c.group, topic, partition)
		require.Equal(t, ErrNoError, block.Err)
		require.Equal(t, c.offset, block.Offset)
	}
}

func TestFuncAdminListOffsets(t *testing.T) {
	t.Parallel()
	checkKafkaVersion(t, "2.1.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	partitionsCount := int32(len(FunctionalTestEnv.KafkaBrokerAddrs) * 3)
	config := NewFunctionalTestConfig()
	config.ClientID = t.Name()
	config.Producer.Partitioner = NewManualPartitioner
	config.Producer.Return.Successes = true

	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, client)

	adminClient, err := NewClusterAdminFromClient(client)
	if err != nil {
		t.Fatal(err)
	}

	topic, err := topicWithEvenLeaders(t, adminClient, client, partitionsCount)
	if err != nil {
		t.Fatalf("failed to create topic with evenly distributed leaders: %v", err)
	}

	const (
		baseTimestamp        = int64(1_700_000_000_000)
		messagesPerPartition = 10
	)
	produceMessagesForPartitions(t, client, topic, partitionsCount, messagesPerPartition, baseTimestamp)

	minOffset := int64(0)
	maxOffset := int64(messagesPerPartition - 1)
	midIndex := int64(messagesPerPartition / 2)
	midTimestamp := baseTimestamp + midIndex*1000

	if err := client.RefreshMetadata(topic); err != nil {
		t.Fatalf("refresh metadata for %s: %v", topic, err)
	}

	listOffsetsAndValidate(
		t,
		adminClient,
		topic,
		partitionsCount,
		OffsetOldest,
		minOffset,
		"earliest",
	)
	listOffsetsAndValidate(
		t,
		adminClient,
		topic,
		partitionsCount,
		OffsetNewest,
		maxOffset+1,
		"latest",
	)

	listOffsetsAndValidate(
		t,
		adminClient,
		topic,
		partitionsCount,
		midTimestamp,
		midIndex,
		"mid timestamp",
	)
}

func TestFuncAdminDeleteRecords(t *testing.T) {
	t.Parallel()
	checkKafkaVersion(t, "0.11.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	const (
		partitionsCount      = int32(1)
		messagesPerPartition = 10
		deleteBeforeOffset   = int64(5)
	)

	config := NewFunctionalTestConfig()
	config.ClientID = t.Name()
	config.Producer.Partitioner = NewManualPartitioner
	config.Producer.Return.Successes = true

	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer safeClose(t, client)

	adminClient, err := NewClusterAdminFromClient(client)
	require.NoError(t, err)

	topic, err := topicWithEvenLeaders(t, adminClient, client, partitionsCount)
	require.NoError(t, err)

	produceMessagesForPartitions(t, client, topic, partitionsCount, messagesPerPartition, time.Now().UnixMilli())

	require.NoError(t, client.RefreshMetadata(topic))

	err = adminClient.DeleteRecords(topic, map[int32]int64{0: deleteBeforeOffset})
	require.NoError(t, err)

	// truncating records before deleteBeforeOffset moves the partition low water mark there
	listOffsetsAndValidate(
		t,
		adminClient,
		topic,
		partitionsCount,
		OffsetOldest,
		deleteBeforeOffset,
		"earliest after delete",
	)
}

func TestFuncAdminAlterConsumerGroupOffsets(t *testing.T) {
	t.Parallel()
	checkKafkaVersion(t, "2.1.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.ClientID = t.Name()

	adminClient, err := NewClusterAdmin(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, adminClient)

	group := testFuncConsumerGroupID(t)
	topic := "test.1"
	partition := int32(0)
	offset := int64(1)

	response, err := adminClient.AlterConsumerGroupOffsets(group, map[string]map[int32]OffsetAndMetadata{
		topic: {partition: {
			Offset:      offset,
			LeaderEpoch: -1,
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if !errors.Is(response.Errors[topic][partition], ErrNoError) {
		t.Fatalf("unexpected error for %s/%d: %v", topic, partition, response.Errors[topic][partition])
	}

	fetched, err := adminClient.ListConsumerGroupOffsets(group, map[string][]int32{topic: {partition}})
	if err != nil {
		t.Fatal(err)
	}

	block := fetched.GetBlock(topic, partition)
	if block == nil {
		t.Fatalf("missing offset for %s/%d", topic, partition)
	}
	if !errors.Is(block.Err, ErrNoError) {
		t.Fatalf("unexpected fetch error for %s/%d: %v", topic, partition, block.Err)
	}
	if block.Offset != offset {
		t.Fatalf("unexpected offset for %s/%d: want %d, got %d", topic, partition, offset, block.Offset)
	}
}

func TestFuncAdminDescribeLogDirs(t *testing.T) {
	t.Parallel()
	checkKafkaVersion(t, "2.0.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	kafkaVersion, err := ParseKafkaVersion(FunctionalTestEnv.KafkaVersion)
	if err != nil {
		t.Fatal(err)
	}

	config := NewFunctionalTestConfig()
	adminClient, err := NewClusterAdmin(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, adminClient)

	brokerIDs := make([]int32, len(FunctionalTestEnv.KafkaBrokerAddrs))
	for i := range brokerIDs {
		brokerIDs[i] = int32(i + 1)
	}

	res, err := adminClient.DescribeLogDirs(brokerIDs)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != len(FunctionalTestEnv.KafkaBrokerAddrs) {
		t.Errorf("should have %d broker replies, got %v\n", len(FunctionalTestEnv.KafkaBrokerAddrs), len(res))
	}

	for _, resp := range res {
		for _, logDir := range resp {
			assert.Equal(t, ErrNoError, logDir.ErrorCode)
			// assert that total bytes and usable bytes were returned for kafka 3.3 and newer
			if kafkaVersion.IsAtLeast(V3_3_0_0) {
				assert.NotZero(t, logDir.TotalBytes)
				assert.NotZero(t, logDir.UsableBytes)
			}
		}
	}
}

func TestFuncAdminDescribeConfig(t *testing.T) {
	t.Parallel()
	checkKafkaVersion(t, "0.11.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	kafkaVersion, err := ParseKafkaVersion(FunctionalTestEnv.KafkaVersion)
	if err != nil {
		t.Fatal(err)
	}

	config := NewFunctionalTestConfig()
	adminClient, err := NewClusterAdmin(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, adminClient)

	// describe all broker configs (nil ConfigNames) to exercise the null
	// compact-array path on the v4 flexible wire format
	results, err := adminClient.DescribeConfigs(
		[]*ConfigResource{{Type: BrokerResource, Name: "1"}},
		DescribeConfigsOptions{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, ErrNoError, results[0].ErrorCode)
	require.NotEmpty(t, results[0].Configs)

	for _, entry := range results[0].Configs {
		assert.NotEmpty(t, entry.Name)
	}

	// IncludeDocumentation, ConfigType and Documentation are available from v3
	// (Kafka 2.6.0); request them via the DescribeConfigs options
	if kafkaVersion.IsAtLeast(V2_6_0_0) {
		documented, err := adminClient.DescribeConfigs(
			[]*ConfigResource{{Type: BrokerResource, Name: "1"}},
			DescribeConfigsOptions{IncludeSynonyms: true, IncludeDocumentation: true},
		)
		require.NoError(t, err)
		require.Len(t, documented, 1)
		require.NotEmpty(t, documented[0].Configs)

		var typed, hasDoc bool
		for _, entry := range documented[0].Configs {
			if entry.Type != UnknownConfigType {
				typed = true
			}
			if entry.Documentation != nil && *entry.Documentation != "" {
				hasDoc = true
			}
		}
		assert.True(t, typed, "expected at least one config with a known ConfigType")
		assert.True(t, hasDoc, "expected at least one config with Documentation")
	}
}

func TestFuncAdminDeleteGroup(t *testing.T) {
	t.Parallel()
	checkKafkaVersion(t, "2.4.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)
	config := NewFunctionalTestConfig()
	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	defer safeClose(t, client)
	if err != nil {
		t.Fatal(err)
	}

	// create a consumer group
	groupID := testFuncConsumerGroupID(t)
	consumerGroup, err := NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, consumerGroup)

	offsetMgr, _ := NewOffsetManagerFromClient(groupID, client)
	defer safeClose(t, offsetMgr)
	markOffset(t, offsetMgr, "test.1", 0, 1)
	offsetMgr.Commit()

	admin, err := NewClusterAdminFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	groups, err := admin.ListConsumerGroups()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := groups[groupID]; !ok {
		t.Fatalf("Expected test group, %s, not found.", groupID)
	}

	err = admin.DeleteConsumerGroup(groupID)
	if err != nil {
		t.Fatal(err)
	}

	groups, err = admin.ListConsumerGroups()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := groups[groupID]; ok {
		t.Fatalf("Expected test group, %s, found after delete.", groupID)
	}
}

func TestFuncAdminDeleteTopic(t *testing.T) {
	t.Parallel()
	checkKafkaVersion(t, "0.10.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	adminClient, err := NewClusterAdmin(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, adminClient)

	err = adminClient.CreateTopic("delete_topic_test", &TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false)
	if err != nil {
		t.Fatal(err)
	}
	err = adminClient.DeleteTopic("delete_topic_test")
	if err != nil {
		t.Fatal(err)
	}
}

// TestFuncAdminElectLeadersV1 covers low-version client compatibility against a
// newer broker for the ElectLeaders V1 non-flexible path. See
// https://github.com/IBM/sarama/pull/3312.
func TestFuncAdminElectLeadersV1(t *testing.T) {
	t.Parallel()
	checkKafkaVersion(t, "2.3.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	// Force ElectLeaders onto request/response version 1 so the broker emits
	// the non-flexible wire format that previously regressed.
	config.Version = V2_3_0_0

	adminClient, err := NewClusterAdmin(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, adminClient)

	results, err := adminClient.ElectLeaders(PreferredElection, map[string][]int32{"test.1": {0}})
	if err != nil {
		t.Fatal(err)
	}

	topicResults, ok := results["test.1"]
	if !ok {
		t.Fatalf("topic test.1 missing in response: %#v", results)
	}

	partitionResult, ok := topicResults[0]
	if !ok {
		t.Fatalf("partition 0 missing in response: %#v", topicResults)
	}

	if partitionResult == nil {
		t.Fatal("partition 0 returned nil result")
	}

	switch partitionResult.ErrorCode {
	case ErrNoError, ErrElectionNotNeeded:
	default:
		t.Fatalf("expected partition 0 to return a decodable ElectLeaders result, got %v (%v)", partitionResult.ErrorCode, partitionResult.ErrorMessage)
	}
}

func TestFuncAdminAlterPartitionReassignments(t *testing.T) {
	t.Parallel()
	const (
		waitFor = 60 * time.Second
		tick    = 500 * time.Millisecond
	)
	checkKafkaVersion(t, "2.4.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.ClientID = t.Name()

	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer safeClose(t, client)

	adminClient, err := NewClusterAdminFromClient(client)
	require.NoError(t, err)

	brokers := client.Brokers()
	require.GreaterOrEqual(t, len(brokers), 2, "need at least two brokers to grow replication factor")
	brokerIDs := make([]int32, 0, len(brokers))
	for _, b := range brokers {
		brokerIDs = append(brokerIDs, b.ID())
	}
	slices.Sort(brokerIDs)

	const numPartitions = int32(2)
	topic := fmt.Sprintf("alter-reassignments-%d", time.Now().UnixNano())
	initial := map[int32][]int32{
		0: {brokerIDs[0]},
		1: {brokerIDs[1]},
	}
	require.NoError(t, adminClient.CreateTopic(topic, &TopicDetail{
		NumPartitions:     -1,
		ReplicationFactor: -1,
		ReplicaAssignment: initial,
	}, false))
	defer func() {
		if err := adminClient.DeleteTopic(topic); err != nil {
			t.Logf("delete topic %q: %v", topic, err)
		}
	}()

	target := [][]int32{
		{brokerIDs[0], brokerIDs[1]},
		{brokerIDs[1], brokerIDs[0]},
	}
	require.NoError(t, adminClient.AlterPartitionReassignments(topic, target))

	partitions := []int32{0, 1}
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		status, err := adminClient.ListPartitionReassignments(topic, partitions)
		require.NoError(t, err)
		// ListPartitionReassignments only returns partitions that are
		// still being reassigned; an empty TopicStatus entry means the
		// move has finished.
		require.Empty(t, status[topic], "reassignment still in progress")
	}, waitFor, tick, "topic %q reassignment did not complete in time", topic)

	require.NoError(t, client.RefreshMetadata(topic))

	require.EventuallyWithT(t, func(t *assert.CollectT) {
		meta, err := adminClient.DescribeTopics([]string{topic})
		require.NoError(t, err)
		require.Len(t, meta, 1)
		require.Equal(t, ErrNoError, meta[0].Err)
		require.Len(t, meta[0].Partitions, int(numPartitions))
		for _, p := range meta[0].Partitions {
			want := target[p.ID]
			require.Equal(t, want, p.Replicas, "partition %d replicas", p.ID)
			require.ElementsMatch(t, want, p.Isr, "partition %d ISR did not catch up", p.ID)
		}
	}, waitFor, tick, "topic %q never reported the expected final replica set", topic)
}

func TestFuncAdminIncrementalAlterConfigs(t *testing.T) {
	t.Parallel()
	checkKafkaVersion(t, "2.3.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	adminClient, err := NewClusterAdmin(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, adminClient)

	brokerIDs := make([]int32, len(FunctionalTestEnv.KafkaBrokerAddrs))
	for i := range brokerIDs {
		brokerIDs[i] = int32(i + 1)
	}

	getConfigValue := func(config string) int {
		resource := ConfigResource{
			Type:        BrokerResource,
			Name:        "1",
			ConfigNames: []string{config},
		}
		res, err := adminClient.DescribeConfig(resource)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 1 {
			t.Fatalf("expected 1 config in response but got %d", len(res))
		}
		if res[0].Name != config {
			t.Fatalf("expected config in response name to be '%s' but got '%s'", config, res[0].Name)
		}
		n, err := strconv.Atoi(res[0].Value)
		if err != nil {
			t.Fatalf("failed to parse config in response value '%s': %v", res[0].Value, err)
		}
		return n
	}
	configName := "log.cleaner.backoff.ms"
	n := getConfigValue(configName)
	n++
	value := fmt.Sprintf("%d", n)
	err = adminClient.IncrementalAlterConfig(BrokerResource, "1",
		map[string]IncrementalAlterConfigsEntry{
			configName: {
				Operation: IncrementalAlterConfigsOperationSet,
				Value:     &value,
			},
		}, false)
	if err != nil {
		t.Fatalf("failed to alter config: %v", err)
	}
}

func TestFuncAdminUpdateFeatures(t *testing.T) {
	t.Parallel()
	// feature updates need a KRaft cluster; ZooKeeper-mode brokers don't
	// advertise any features
	checkKafkaVersion(t, "4.0.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	kafkaVersion, err := ParseKafkaVersion(FunctionalTestEnv.KafkaVersion)
	require.NoError(t, err)

	config := NewFunctionalTestConfig()
	config.Version = kafkaVersion
	adminClient, err := NewClusterAdmin(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer safeClose(t, adminClient)

	// an update for a made-up feature name should be rejected without
	// touching cluster state; the controller fails the whole batch with
	// INVALID_UPDATE_VERSION when every update in it failed
	_, err = adminClient.UpdateFeatures([]FeatureUpdate{{
		Feature:         "sarama.unsupported.feature",
		MaxVersionLevel: 1,
	}})
	require.ErrorIs(t, err, ErrInvalidUpdateVersion)

	// share.version (KIP-932) ships disabled on 4.1 (finalized 0, max 1),
	// so we can do a real upgrade here
	if kafkaVersion.IsAtLeast(V4_1_0_0) && !kafkaVersion.IsAtLeast(V4_2_0_0) {
		results, err := adminClient.UpdateFeatures([]FeatureUpdate{{
			Feature:         "share.version",
			MaxVersionLevel: 1,
		}})
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, "share.version", results[0].Feature)
		assert.Equal(t, ErrNoError, results[0].ErrorCode)
	}
}

//go:build functional

package sarama

import (
	"context"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func TestFuncAdminQuotas(t *testing.T) {
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

	// Check that we now have a quota entry
	quotas, err = adminClient.DescribeClientQuotas(nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(quotas) == 0 {
		t.Fatal("Expected not empty quotas")
	}
	if len(quotas) > 1 {
		t.Fatalf("Expected one quota entry, found: %v", quotas)
	}

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
	quotas, err = adminClient.DescribeClientQuotas([]QuotaFilterComponent{userFilter, clientFilter}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(quotas) == 0 {
		t.Fatal("Expected not empty quotas")
	}
	if len(quotas) > 1 {
		t.Fatalf("Expected one quota entry, found: %v", quotas)
	}
	if quotas[0].Values[consumeOp.Key] != consumeOp.Value {
		t.Fatalf("Expected specific quota value to be %f, found: %v", consumeOp.Value, quotas[0].Values[consumeOp.Key])
	}

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
	if err := stopDockerTestBroker(context.Background(), brokerID); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(
		func() {
			if err := startDockerTestBroker(context.Background(), brokerID); err != nil {
				t.Fatal(err)
			}
		},
	)

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

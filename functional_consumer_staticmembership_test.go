//go:build functional

package sarama

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"sync/atomic"
	"testing"
)

func TestFuncConsumerGroupStaticMembership_Basic(t *testing.T) {
	checkKafkaVersion(t, "2.3.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)
	groupID := testFuncConsumerGroupID(t)

	t.Helper()

	config1 := NewFunctionalTestConfig()
	config1.ClientID = "M1"
	config1.Consumer.Offsets.Initial = OffsetNewest
	config1.Consumer.Group.InstanceId = "Instance1"
	m1 := runTestFuncConsumerGroupMemberWithConfig(t, config1, groupID, 100, nil, "test.4")
	defer m1.Close()

	config2 := NewFunctionalTestConfig()
	config2.ClientID = "M2"
	config2.Consumer.Offsets.Initial = OffsetNewest
	config2.Consumer.Group.InstanceId = "Instance2"
	m2 := runTestFuncConsumerGroupMemberWithConfig(t, config2, groupID, 100, nil, "test.4")
	defer m2.Close()

	m1.WaitForState(2)
	m2.WaitForState(2)

	err := testFuncConsumerGroupProduceMessage("test.4", 1000)
	if err != nil {
		t.Fatal(err)
	}

	admin, err := NewClusterAdmin(FunctionalTestEnv.KafkaBrokerAddrs, config1)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, admin)

	res, err := admin.DescribeConsumerGroups([]string{groupID})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Errorf("group description should be only 1, got %v\n", len(res))
	}
	if len(res[0].Members) != 2 {
		t.Errorf("should have 2 members in group , got %v\n", len(res[0].Members))
	}

	m1.WaitForState(4)
	m2.WaitForState(4)

	m1.AssertCleanShutdown()
	m2.AssertCleanShutdown()
}

func TestFuncConsumerGroupStaticMembership_RejoinAndLeave(t *testing.T) {
	checkKafkaVersion(t, "2.4.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)
	groupID := testFuncConsumerGroupID(t)

	t.Helper()

	config1 := NewFunctionalTestConfig()
	config1.ClientID = "M1"
	config1.Consumer.Offsets.Initial = OffsetNewest
	config1.Consumer.Group.InstanceId = "Instance1"
	m1 := runTestFuncConsumerGroupMemberWithConfig(t, config1, groupID, math.MaxInt32, nil, "test.4")
	defer m1.Close()

	config2 := NewFunctionalTestConfig()
	config2.ClientID = "M2"
	config2.Consumer.Offsets.Initial = OffsetNewest
	config2.Consumer.Group.InstanceId = "Instance2"
	m2 := runTestFuncConsumerGroupMemberWithConfig(t, config2, groupID, math.MaxInt32, nil, "test.4")
	defer m2.Close()

	m1.WaitForState(2)
	m2.WaitForState(2)

	admin, err := NewClusterAdmin(FunctionalTestEnv.KafkaBrokerAddrs, config1)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, admin)

	res1, err := admin.DescribeConsumerGroups([]string{groupID})
	if err != nil {
		t.Fatal(err)
	}
	if len(res1) != 1 {
		t.Errorf("group description should be only 1, got %v\n", len(res1))
	}
	if len(res1[0].Members) != 2 {
		t.Errorf("should have 2 members in group , got %v\n", len(res1[0].Members))
	}

	generationId1 := m1.generationId

	// shut down m2, membership should not change (we didn't leave group when close)
	m2.AssertCleanShutdown()

	res2, err := admin.DescribeConsumerGroups([]string{groupID})
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(res1, res2) {
		res1Bytes, _ := json.Marshal(res1)
		res2Bytes, _ := json.Marshal(res2)
		t.Errorf("group description be the same before %s, after %s", res1Bytes, res2Bytes)
	}

	generationId2 := atomic.LoadInt32(&m1.generationId)
	if generationId2 != generationId1 {
		t.Errorf("m1 generation should not increase expect %v, actual %v", generationId1, generationId2)
	}

	// m2 rejoin, should generate a new memberId, no re-balance happens
	m2 = runTestFuncConsumerGroupMemberWithConfig(t, config2, groupID, math.MaxInt32, nil, "test.4")
	m2.WaitForState(2)
	m1.WaitForState(2)

	res3, err := admin.DescribeConsumerGroups([]string{groupID})
	if err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(res3) != 1 {
		t.Errorf("group description should be only 1, got %v\n", len(res3))
	}
	if len(res3[0].Members) != 2 {
		t.Errorf("should have 2 members in group , got %v\n", len(res3[0].Members))
	}

	generationId3 := atomic.LoadInt32(&m1.generationId)
	if generationId3 != generationId1 {
		t.Errorf("m1 generation should not increase expect %v, actual %v", generationId1, generationId2)
	}

	m2.AssertCleanShutdown()
	removeResp, err := admin.RemoveMemberFromConsumerGroup(groupID, []string{config2.Consumer.Group.InstanceId})
	if err != nil {
		t.Fatal(err)
	}
	if removeResp.Err != ErrNoError {
		t.Errorf("remove %s from consumer group failed %v", config2.Consumer.Group.InstanceId, removeResp.Err)
	}
	m1.WaitForHandlers(4)

	generationId4 := atomic.LoadInt32(&m1.generationId)
	if generationId4 == generationId1 {
		t.Errorf("m1 generation should increase expect %v, actual %v", generationId1, generationId2)
	}
}

func TestFuncConsumerGroupStaticMembership_Fenced(t *testing.T) {
	checkKafkaVersion(t, "2.3.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)
	groupID := testFuncConsumerGroupID(t)

	t.Helper()

	config1 := NewFunctionalTestConfig()
	config1.ClientID = "M1"
	config1.Consumer.Offsets.Initial = OffsetNewest
	config1.Consumer.Group.InstanceId = "Instance1"
	m1 := runTestFuncConsumerGroupMemberWithConfig(t, config1, groupID, math.MaxInt32, nil, "test.4")
	defer m1.Close()

	config2 := NewFunctionalTestConfig()
	config2.ClientID = "M2"
	config2.Consumer.Offsets.Initial = OffsetNewest
	config2.Consumer.Group.InstanceId = "Instance2"
	m2 := runTestFuncConsumerGroupMemberWithConfig(t, config2, groupID, math.MaxInt32, nil, "test.4")
	defer m2.Close()

	m1.WaitForState(2)
	m2.WaitForState(2)

	config3 := NewFunctionalTestConfig()
	config3.ClientID = "M3"
	config3.Consumer.Offsets.Initial = OffsetNewest
	config3.Consumer.Group.InstanceId = "Instance2" // same instance id as config2

	m3 := runTestFuncConsumerGroupMemberWithConfig(t, config3, groupID, math.MaxInt32, nil, "test.4")
	defer m3.Close()

	m3.WaitForState(2)

	m2.WaitForState(4)
	if len(m2.errs) < 1 {
		t.Errorf("expect m2 to be fenced by group instanced id, but got no err")
	}
	if !errors.Is(m2.errs[0], ErrFencedInstancedId) {
		t.Errorf("expect m2 to be fenced by group instanced id, but got wrong err %v", m2.errs[0])
	}

	m1.AssertCleanShutdown()
	m3.AssertCleanShutdown()
}

// --------------------------------------------------------------------

func testFuncConsumerGroupProduceMessage(topic string, count int) error {
	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, NewFunctionalTestConfig())
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	producer, err := NewAsyncProducerFromClient(client)
	if err != nil {
		return err
	}
	for i := 0; i < count; i++ {
		producer.Input() <- &ProducerMessage{Topic: topic, Value: ByteEncoder([]byte("testdata"))}
	}
	return producer.Close()
}

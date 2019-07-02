package sarama

import (
	"errors"
	"strings"
	"testing"
)

func TestClusterAdmin(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
	})

	config := NewConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminInvalidController(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
	})

	config := NewConfig()
	config.Version = V1_0_0_0
	_, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err == nil {
		t.Fatal(errors.New("controller not set still cluster admin was created"))
	}

	if err != ErrControllerNotAvailable {
		t.Fatal(err)
	}
}

func TestClusterAdminCreateTopic(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"CreateTopicsRequest": NewMockCreateTopicsResponse(t),
	})

	config := NewConfig()
	config.Version = V0_10_2_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	err = admin.CreateTopic("my_topic", &TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminCreateTopicWithInvalidTopicDetail(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"CreateTopicsRequest": NewMockCreateTopicsResponse(t),
	})

	config := NewConfig()
	config.Version = V0_10_2_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.CreateTopic("my_topic", nil, false)
	if err.Error() != "you must specify topic details" {
		t.Fatal(err)
	}
	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminCreateTopicWithoutAuthorization(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"CreateTopicsRequest": NewMockCreateTopicsResponse(t),
	})

	config := NewConfig()
	config.Version = V0_11_0_0

	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.CreateTopic("_internal_topic", &TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false)
	want := "insufficient permissions to create topic with reserved prefix"
	if !strings.HasSuffix(err.Error(), want) {
		t.Fatal(err)
	}
	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminListTopics(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetLeader("my_topic", 0, seedBroker.BrokerID()),
		"DescribeConfigsRequest": NewMockDescribeConfigsResponse(t),
	})

	config := NewConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	entries, err := admin.ListTopics()
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) <= 0 {
		t.Fatal(errors.New("no resource present"))
	}

	topic, found := entries["my_topic"]
	if !found {
		t.Fatal(errors.New("topic not found in response"))
	}
	_, found = topic.ConfigEntries["max.message.bytes"]
	if found {
		t.Fatal(errors.New("default topic config entry incorrectly found in response"))
	}
	value, _ := topic.ConfigEntries["retention.ms"]
	if value == nil || *value != "5000" {
		t.Fatal(errors.New("non-default topic config entry not found in response"))
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}

	if topic.ReplicaAssignment == nil || topic.ReplicaAssignment[0][0] != 1 {
		t.Fatal(errors.New("replica assignment not found in response"))
	}
}

func TestClusterAdminDeleteTopic(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"DeleteTopicsRequest": NewMockDeleteTopicsResponse(t),
	})

	config := NewConfig()
	config.Version = V0_10_2_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.DeleteTopic("my_topic")
	if err != nil {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminDeleteEmptyTopic(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"DeleteTopicsRequest": NewMockDeleteTopicsResponse(t),
	})

	config := NewConfig()
	config.Version = V0_10_2_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.DeleteTopic("")
	if err != ErrInvalidTopic {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminCreatePartitions(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"CreatePartitionsRequest": NewMockCreatePartitionsResponse(t),
	})

	config := NewConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.CreatePartitions("my_topic", 3, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminCreatePartitionsWithDiffVersion(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"CreatePartitionsRequest": NewMockCreatePartitionsResponse(t),
	})

	config := NewConfig()
	config.Version = V0_10_2_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.CreatePartitions("my_topic", 3, nil, false)
	if err != ErrUnsupportedVersion {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminCreatePartitionsWithoutAuthorization(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"CreatePartitionsRequest": NewMockCreatePartitionsResponse(t),
	})

	config := NewConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.CreatePartitions("_internal_topic", 3, nil, false)
	want := "insufficient permissions to create partition on topic with reserved prefix"
	if !strings.HasSuffix(err.Error(), want) {
		t.Fatal(err)
	}
	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminDeleteRecords(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"DeleteRecordsRequest": NewMockDeleteRecordsResponse(t),
	})

	config := NewConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	partitionOffset := make(map[int32]int64)
	partitionOffset[1] = 1000
	partitionOffset[2] = 1000
	partitionOffset[3] = 1000

	err = admin.DeleteRecords("my_topic", partitionOffset)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminDeleteRecordsWithDiffVersion(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"DeleteRecordsRequest": NewMockDeleteRecordsResponse(t),
	})

	config := NewConfig()
	config.Version = V0_10_2_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	partitionOffset := make(map[int32]int64)
	partitionOffset[1] = 1000
	partitionOffset[2] = 1000
	partitionOffset[3] = 1000

	err = admin.DeleteRecords("my_topic", partitionOffset)
	if err != ErrUnsupportedVersion {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminDescribeConfig(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"DescribeConfigsRequest": NewMockDescribeConfigsResponse(t),
	})

	config := NewConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	resource := ConfigResource{Name: "r1", Type: TopicResource, ConfigNames: []string{"my_topic"}}
	entries, err := admin.DescribeConfig(resource)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) <= 0 {
		t.Fatal(errors.New("no resource present"))
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminAlterConfig(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"AlterConfigsRequest": NewMockAlterConfigsResponse(t),
	})

	config := NewConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	var value string
	entries := make(map[string]*string)
	value = "3"
	entries["ReplicationFactor"] = &value
	err = admin.AlterConfig(TopicResource, "my_topic", entries, false)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminCreateAcl(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"CreateAclsRequest": NewMockCreateAclsResponse(t),
	})

	config := NewConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	r := Resource{ResourceType: AclResourceTopic, ResourceName: "my_topic"}
	a := Acl{Host: "localhost", Operation: AclOperationAlter, PermissionType: AclPermissionAny}

	err = admin.CreateACL(r, a)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminListAcls(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"DescribeAclsRequest": NewMockListAclsResponse(t),
		"CreateAclsRequest":   NewMockCreateAclsResponse(t),
	})

	config := NewConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	r := Resource{ResourceType: AclResourceTopic, ResourceName: "my_topic"}
	a := Acl{Host: "localhost", Operation: AclOperationAlter, PermissionType: AclPermissionAny}

	err = admin.CreateACL(r, a)
	if err != nil {
		t.Fatal(err)
	}
	resourceName := "my_topic"
	filter := AclFilter{
		ResourceType: AclResourceTopic,
		Operation:    AclOperationRead,
		ResourceName: &resourceName,
	}

	rAcls, err := admin.ListAcls(filter)
	if err != nil {
		t.Fatal(err)
	}
	if len(rAcls) <= 0 {
		t.Fatal("no acls present")
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminDeleteAcl(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"DeleteAclsRequest": NewMockDeleteAclsResponse(t),
	})

	config := NewConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	resourceName := "my_topic"
	filter := AclFilter{
		ResourceType: AclResourceTopic,
		Operation:    AclOperationAlter,
		ResourceName: &resourceName,
	}

	_, err = admin.DeleteACL(filter, false)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDescribeTopic(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetLeader("my_topic", 0, seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
	})

	config := NewConfig()
	config.Version = V1_0_0_0

	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	topics, err := admin.DescribeTopics([]string{"my_topic"})
	if err != nil {
		t.Fatal(err)
	}

	if len(topics) != 1 {
		t.Fatalf("Expected 1 result, got %v", len(topics))
	}

	if topics[0].Name != "my_topic" {
		t.Fatalf("Incorrect topic name: %v", topics[0].Name)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDescribeTopicWithVersion0_11(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetLeader("my_topic", 0, seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
	})

	config := NewConfig()
	config.Version = V0_11_0_0

	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	topics, err := admin.DescribeTopics([]string{"my_topic"})
	if err != nil {
		t.Fatal(err)
	}

	if len(topics) != 1 {
		t.Fatalf("Expected 1 result, got %v", len(topics))
	}

	if topics[0].Name != "my_topic" {
		t.Fatalf("Incorrect topic name: %v", topics[0].Name)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDescribeConsumerGroup(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	expectedGroupID := "my-group"

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"DescribeGroupsRequest": NewMockDescribeGroupsResponse(t).AddGroupDescription(expectedGroupID, &GroupDescription{
			GroupId: expectedGroupID,
		}),
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).SetCoordinator(CoordinatorGroup, expectedGroupID, seedBroker),
	})

	config := NewConfig()
	config.Version = V1_0_0_0

	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	result, err := admin.DescribeConsumerGroups([]string{expectedGroupID})
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 result, got %v", len(result))
	}

	if result[0].GroupId != expectedGroupID {
		t.Fatalf("Expected groupID %v, got %v", expectedGroupID, result[0].GroupId)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestListConsumerGroups(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"ListGroupsRequest": NewMockListGroupsResponse(t).
			AddGroup("my-group", "consumer"),
	})

	config := NewConfig()
	config.Version = V1_0_0_0

	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	groups, err := admin.ListConsumerGroups()
	if err != nil {
		t.Fatal(err)
	}

	if len(groups) != 1 {
		t.Fatalf("Expected %v results, got %v", 1, len(groups))
	}

	protocolType, ok := groups["my-group"]

	if !ok {
		t.Fatal("Expected group to be returned, but it did not")
	}

	if protocolType != "consumer" {
		t.Fatalf("Expected protocolType %v, got %v", "consumer", protocolType)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestListConsumerGroupsMultiBroker(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	secondBroker := NewMockBroker(t, 2)
	defer secondBroker.Close()

	firstGroup := "first"
	secondGroup := "second"
	nonExistingGroup := "non-existing-group"

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.BrokerID()),
		"ListGroupsRequest": NewMockListGroupsResponse(t).
			AddGroup(firstGroup, "consumer"),
	})

	secondBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.BrokerID()),
		"ListGroupsRequest": NewMockListGroupsResponse(t).
			AddGroup(secondGroup, "consumer"),
	})

	config := NewConfig()
	config.Version = V1_0_0_0

	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	groups, err := admin.ListConsumerGroups()
	if err != nil {
		t.Fatal(err)
	}

	if len(groups) != 2 {
		t.Fatalf("Expected %v results, got %v", 1, len(groups))
	}

	if _, found := groups[firstGroup]; !found {
		t.Fatalf("Expected group %v to be present in result set, but it isn't", firstGroup)
	}

	if _, found := groups[secondGroup]; !found {
		t.Fatalf("Expected group %v to be present in result set, but it isn't", secondGroup)
	}

	if _, found := groups[nonExistingGroup]; found {
		t.Fatalf("Expected group %v to not exist, but it exists", nonExistingGroup)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestListConsumerGroupOffsets(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	group := "my-group"
	topic := "my-topic"
	partition := int32(0)
	expectedOffset := int64(0)

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"OffsetFetchRequest": NewMockOffsetFetchResponse(t).SetOffset(group, "my-topic", partition, expectedOffset, "", ErrNoError).SetError(ErrNoError),
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).SetCoordinator(CoordinatorGroup, group, seedBroker),
	})

	config := NewConfig()
	config.Version = V1_0_0_0

	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	response, err := admin.ListConsumerGroupOffsets(group, map[string][]int32{
		topic: []int32{0},
	})
	if err != nil {
		t.Fatalf("ListConsumerGroupOffsets failed with error %v", err)
	}

	block := response.GetBlock(topic, partition)
	if block == nil {
		t.Fatalf("Expected block for topic %v and partition %v to exist, but it doesn't", topic, partition)
	}

	if block.Offset != expectedOffset {
		t.Fatalf("Expected offset %v, got %v", expectedOffset, block.Offset)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestDeleteConsumerGroup(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	group := "my-group"

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		// "OffsetFetchRequest":  NewMockOffsetFetchResponse(t).SetOffset(group, "my-topic", partition, expectedOffset, "", ErrNoError),
		"DeleteGroupsRequest": NewMockDeleteGroupsRequest(t).SetDeletedGroups([]string{group}),
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).SetCoordinator(CoordinatorGroup, group, seedBroker),
	})

	config := NewConfig()
	config.Version = V1_1_0_0

	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.DeleteConsumerGroup(group)
	if err != nil {
		t.Fatalf("DeleteConsumerGroup failed with error %v", err)
	}

}

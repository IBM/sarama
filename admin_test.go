package sarama

import (
	"errors"
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
	defer admin.Close()
	err = admin.CreateTopic("my_topic", &TopicDetail{NumPartitions: 1, ReplicationFactor: 1})
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
	defer admin.Close()

	err = admin.CreateTopic("my_topic", nil)
	if err != ErrInvalidInput {
		t.Fatal(err)
	}
}

func TestClusterAdminCreateTopicWithDiffVersion(t *testing.T) {
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
	defer admin.Close()

	err = admin.CreateTopic("my_topic", &TopicDetail{NumPartitions: 1, ReplicationFactor: 1})
	if err != ErrInsufficientData {
		t.Fatal(err)
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
	defer admin.Close()

	err = admin.CreatePartitions("my_topic", 3, nil, false)
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
	defer admin.Close()

	err = admin.CreatePartitions("my_topic", 3, nil, false)
	if err != ErrUnsupportedVersion {
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
	defer admin.Close()

	partitionOffset := make(map[int32]int64)
	partitionOffset[1] = 1000
	partitionOffset[2] = 1000
	partitionOffset[3] = 1000

	err = admin.DeleteRecords("my_topic", partitionOffset)
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
	defer admin.Close()

	partitionOffset := make(map[int32]int64)
	partitionOffset[1] = 1000
	partitionOffset[2] = 1000
	partitionOffset[3] = 1000

	err = admin.DeleteRecords("my_topic", partitionOffset)
	if err != ErrUnsupportedVersion {
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
	defer admin.Close()

	resource := ConfigResource{Name: "r1", Type: TopicResource, ConfigNames: []string{"my_topic"}}
	entries, err := admin.DescribeConfig(resource)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) <= 0 {
		t.Fatal(errors.New("no resource present"))
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
	defer admin.Close()

	var value string
	entries := make(map[string]*string)
	value = "3"
	entries["ReplicationFactor"] = &value
	err = admin.AlterConfig(TopicResource, "my_topic", entries, false)
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
	defer admin.Close()

	r := Resource{ResourceType: AclResourceTopic, ResourceName: "my_topic"}
	a := Acl{Host: "localhost", Operation: AclOperationAlter, PermissionType: AclPermissionAny}

	err = admin.CreateAcl(r, a)
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminListAcls(t *testing.T) {
	//have to be filled once the implementation is done
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
	defer admin.Close()

	resourceName := "my_topic"
	filter := AclFilter{
		ResourceType: AclResourceTopic,
		Operation:    AclOperationAlter,
		ResourceName: &resourceName,
	}

	_, err = admin.DeleteAcl(filter, false)
	if err != nil {
		t.Fatal(err)
	}
}

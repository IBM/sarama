package sarama

import (
	"errors"
	"testing"
)

func TestClusterAdmin(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	seedBroker.Returns(metadataResponse)

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
	leader.Close()
	seedBroker.Close()
}

func TestClusterAdminCreateTopic(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest":     NewMockMetadataResponse(t),
		"CreateTopicsRequest": NewMockCreateTopicsResponse(t),
	})

	config := NewConfig()
	config.Version = V0_10_2_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	err = admin.CreateTopic("my_topic", &TopicDetail{NumPartitions: 1, ReplicationFactor: 1})
	if err != nil {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()
}

func TestClusterAdminCreateTopicWithInvalidTopicDetail(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest":     NewMockMetadataResponse(t),
		"CreateTopicsRequest": NewMockCreateTopicsResponse(t),
	})

	config := NewConfig()
	config.Version = V0_10_2_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.CreateTopic("my_topic", nil)
	if err != ErrInvalidInput {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()
}

func TestClusterAdminCreateTopicWithDiffVersion(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest":     NewMockMetadataResponse(t),
		"CreateTopicsRequest": NewMockCreateTopicsResponse(t),
	})

	config := NewConfig()
	config.Version = V0_11_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.CreateTopic("my_topic", &TopicDetail{NumPartitions: 1, ReplicationFactor: 1})
	if err != ErrInsufficientData {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()
}

func TestClusterAdminDeleteTopic(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest":     NewMockMetadataResponse(t),
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
	seedBroker.Close()
}

func TestClusterAdminCreatePartitions(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest":         NewMockMetadataResponse(t),
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
	seedBroker.Close()
}

func TestClusterAdminCreatePartitionsWithDiffVersion(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest":         NewMockMetadataResponse(t),
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
	seedBroker.Close()
}

func TestClusterAdminDeleteRecords(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest":      NewMockMetadataResponse(t),
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
	seedBroker.Close()
}

func TestClusterAdminDeleteRecordsWithDiffVersion(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest":      NewMockMetadataResponse(t),
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
	seedBroker.Close()
}

func TestClusterAdminDescribeConfig(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest":        NewMockMetadataResponse(t),
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
	seedBroker.Close()
}

func TestClusterAdminAlterConfig(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest":     NewMockMetadataResponse(t),
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
	seedBroker.Close()
}

func TestClusterAdminCreateAcl(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest":   NewMockMetadataResponse(t),
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

	err = admin.CreateAcl(r, a)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()
}

func TestClusterAdminListAcls(t *testing.T) {
	//have to be filled once the implementation is done
}

func TestClusterAdminDeleteAcl(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest":   NewMockMetadataResponse(t),
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

	_, err = admin.DeleteAcl(filter, false)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()
}

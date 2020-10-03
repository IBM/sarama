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

	config := NewTestConfig()
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

	config := NewTestConfig()
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

	config := NewTestConfig()
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

	config := NewTestConfig()
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

	config := NewTestConfig()
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

	config := NewTestConfig()
	config.Version = V1_1_0_0
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
	value := topic.ConfigEntries["retention.ms"]
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

	config := NewTestConfig()
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

	config := NewTestConfig()
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

	config := NewTestConfig()
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

	config := NewTestConfig()
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

	config := NewTestConfig()
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

func TestClusterAdminAlterPartitionReassignments(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	secondBroker := NewMockBroker(t, 2)
	defer secondBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(secondBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.BrokerID()),
	})

	secondBroker.SetHandlerByMap(map[string]MockResponse{
		"AlterPartitionReassignmentsRequest": NewMockAlterPartitionReassignmentsResponse(t),
	})

	config := NewTestConfig()
	config.Version = V2_4_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	var topicAssignment = make([][]int32, 0, 3)

	err = admin.AlterPartitionReassignments("my_topic", topicAssignment)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminAlterPartitionReassignmentsWithDiffVersion(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	secondBroker := NewMockBroker(t, 2)
	defer secondBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(secondBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.BrokerID()),
	})

	secondBroker.SetHandlerByMap(map[string]MockResponse{
		"AlterPartitionReassignmentsRequest": NewMockAlterPartitionReassignmentsResponse(t),
	})

	config := NewTestConfig()
	config.Version = V2_3_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	var topicAssignment = make([][]int32, 0, 3)

	err = admin.AlterPartitionReassignments("my_topic", topicAssignment)

	if !strings.ContainsAny(err.Error(), ErrUnsupportedVersion.Error()) {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminListPartitionReassignments(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	secondBroker := NewMockBroker(t, 2)
	defer secondBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(secondBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.BrokerID()),
	})

	secondBroker.SetHandlerByMap(map[string]MockResponse{
		"ListPartitionReassignmentsRequest": NewMockListPartitionReassignmentsResponse(t),
	})

	config := NewTestConfig()
	config.Version = V2_4_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	response, err := admin.ListPartitionReassignments("my_topic", []int32{0, 1})
	if err != nil {
		t.Fatal(err)
	}

	partitionStatus, ok := response["my_topic"]
	if !ok {
		t.Fatalf("topic missing in response")
	} else {
		if len(partitionStatus) != 2 {
			t.Fatalf("partition missing in response")
		}
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminListPartitionReassignmentsWithDiffVersion(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	secondBroker := NewMockBroker(t, 2)
	defer secondBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(secondBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.BrokerID()),
	})

	secondBroker.SetHandlerByMap(map[string]MockResponse{
		"ListPartitionReassignmentsRequest": NewMockListPartitionReassignmentsResponse(t),
	})

	config := NewTestConfig()
	config.Version = V2_3_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	var partitions = make([]int32, 0)

	_, err = admin.ListPartitionReassignments("my_topic", partitions)

	if !strings.ContainsAny(err.Error(), ErrUnsupportedVersion.Error()) {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminDeleteRecords(t *testing.T) {
	topicName := "my_topic"
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetLeader(topicName, 1, 1).
			SetLeader(topicName, 2, 1).
			SetLeader(topicName, 3, 1),
		"DeleteRecordsRequest": NewMockDeleteRecordsResponse(t),
	})

	config := NewTestConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	partitionOffsetFake := make(map[int32]int64)
	partitionOffsetFake[4] = 1000
	errFake := admin.DeleteRecords(topicName, partitionOffsetFake)
	if errFake == nil {
		t.Fatal(err)
	}

	partitionOffset := make(map[int32]int64)
	partitionOffset[1] = 1000
	partitionOffset[2] = 1000
	partitionOffset[3] = 1000

	err = admin.DeleteRecords(topicName, partitionOffset)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminDeleteRecordsWithInCorrectBroker(t *testing.T) {
	topicName := "my_topic"
	seedBroker := NewMockBroker(t, 1)
	secondBroker := NewMockBroker(t, 2)
	defer seedBroker.Close()
	defer secondBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.brokerID).
			SetLeader(topicName, 1, 1).
			SetLeader(topicName, 2, 1).
			SetLeader(topicName, 3, 2),
		"DeleteRecordsRequest": NewMockDeleteRecordsResponse(t),
	})

	secondBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.brokerID).
			SetLeader(topicName, 1, 1).
			SetLeader(topicName, 2, 1).
			SetLeader(topicName, 3, 2),
		"DeleteRecordsRequest": NewMockDeleteRecordsResponse(t),
	})

	config := NewTestConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	partitionOffset := make(map[int32]int64)
	partitionOffset[1] = 1000
	partitionOffset[2] = 1000
	partitionOffset[3] = 1000

	err = admin.DeleteRecords(topicName, partitionOffset)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminDeleteRecordsWithDiffVersion(t *testing.T) {
	topicName := "my_topic"
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetLeader(topicName, 1, 1).
			SetLeader(topicName, 2, 1).
			SetLeader(topicName, 3, 1),
		"DeleteRecordsRequest": NewMockDeleteRecordsResponse(t),
	})

	config := NewTestConfig()
	config.Version = V0_10_2_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	partitionOffset := make(map[int32]int64)
	partitionOffset[1] = 1000
	partitionOffset[2] = 1000
	partitionOffset[3] = 1000

	err = admin.DeleteRecords(topicName, partitionOffset)
	if !strings.HasPrefix(err.Error(), "kafka server: failed to delete records") {
		t.Fatal(err)
	}
	deleteRecordsError, ok := err.(ErrDeleteRecords)

	if !ok {
		t.Fatal(err)
	}

	for _, err := range *deleteRecordsError.Errors {
		if err != ErrUnsupportedVersion {
			t.Fatal(err)
		}
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

	var tests = []struct {
		saramaVersion   KafkaVersion
		requestVersion  int16
		includeSynonyms bool
	}{
		{V1_0_0_0, 0, false},
		{V1_1_0_0, 1, true},
		{V1_1_1_0, 1, true},
		{V2_0_0_0, 2, true},
	}
	for _, tt := range tests {
		config := NewTestConfig()
		config.Version = tt.saramaVersion
		admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = admin.Close()
		}()

		resource := ConfigResource{
			Name:        "r1",
			Type:        TopicResource,
			ConfigNames: []string{"my_topic"},
		}

		entries, err := admin.DescribeConfig(resource)
		if err != nil {
			t.Fatal(err)
		}

		history := seedBroker.History()
		describeReq, ok := history[len(history)-1].Request.(*DescribeConfigsRequest)
		if !ok {
			t.Fatal("failed to find DescribeConfigsRequest in mockBroker history")
		}

		if describeReq.Version != tt.requestVersion {
			t.Fatalf(
				"requestVersion %v did not match expected %v",
				describeReq.Version, tt.requestVersion)
		}

		if len(entries) <= 0 {
			t.Fatal(errors.New("no resource present"))
		}
		if tt.includeSynonyms {
			if len(entries[0].Synonyms) == 0 {
				t.Fatal("expected synonyms to have been included")
			}
		}
	}
}

func TestClusterAdminDescribeConfigWithErrorCode(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"DescribeConfigsRequest": NewMockDescribeConfigsResponseWithErrorCode(t),
	})

	config := NewTestConfig()
	config.Version = V1_1_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = admin.Close()
	}()

	resource := ConfigResource{
		Name:        "r1",
		Type:        TopicResource,
		ConfigNames: []string{"my_topic"},
	}

	_, err = admin.DescribeConfig(resource)
	if err == nil {
		t.Fatal(errors.New("ErrorCode present but no Error returned"))
	}
}

// TestClusterAdminDescribeBrokerConfig ensures that a describe broker config
// is sent to the broker in the resource struct, _not_ the controller
func TestClusterAdminDescribeBrokerConfig(t *testing.T) {
	controllerBroker := NewMockBroker(t, 1)
	defer controllerBroker.Close()
	configBroker := NewMockBroker(t, 2)
	defer configBroker.Close()

	controllerBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(controllerBroker.BrokerID()).
			SetBroker(controllerBroker.Addr(), controllerBroker.BrokerID()).
			SetBroker(configBroker.Addr(), configBroker.BrokerID()),
	})

	configBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(controllerBroker.BrokerID()).
			SetBroker(controllerBroker.Addr(), controllerBroker.BrokerID()).
			SetBroker(configBroker.Addr(), configBroker.BrokerID()),
		"DescribeConfigsRequest": NewMockDescribeConfigsResponse(t),
	})

	config := NewTestConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin(
		[]string{
			controllerBroker.Addr(),
			configBroker.Addr(),
		}, config)
	if err != nil {
		t.Fatal(err)
	}

	for _, resourceType := range []ConfigResourceType{BrokerResource, BrokerLoggerResource} {
		resource := ConfigResource{Name: "2", Type: resourceType}
		entries, err := admin.DescribeConfig(resource)
		if err != nil {
			t.Fatal(err)
		}

		if len(entries) <= 0 {
			t.Fatal(errors.New("no resource present"))
		}
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

	config := NewTestConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	var value string
	entries := make(map[string]*string)
	value = "60000"
	entries["retention.ms"] = &value
	err = admin.AlterConfig(TopicResource, "my_topic", entries, false)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminAlterConfigWithErrorCode(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"AlterConfigsRequest": NewMockAlterConfigsResponseWithErrorCode(t),
	})

	config := NewTestConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = admin.Close()
	}()

	var value string
	entries := make(map[string]*string)
	value = "60000"
	entries["retention.ms"] = &value
	err = admin.AlterConfig(TopicResource, "my_topic", entries, false)
	if err == nil {
		t.Fatal(errors.New("ErrorCode present but no Error returned"))
	}
}

func TestClusterAdminAlterBrokerConfig(t *testing.T) {
	controllerBroker := NewMockBroker(t, 1)
	defer controllerBroker.Close()
	configBroker := NewMockBroker(t, 2)
	defer configBroker.Close()

	controllerBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(controllerBroker.BrokerID()).
			SetBroker(controllerBroker.Addr(), controllerBroker.BrokerID()).
			SetBroker(configBroker.Addr(), configBroker.BrokerID()),
	})
	configBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(controllerBroker.BrokerID()).
			SetBroker(controllerBroker.Addr(), controllerBroker.BrokerID()).
			SetBroker(configBroker.Addr(), configBroker.BrokerID()),
		"AlterConfigsRequest": NewMockAlterConfigsResponse(t),
	})

	config := NewTestConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin(
		[]string{
			controllerBroker.Addr(),
			configBroker.Addr(),
		}, config)
	if err != nil {
		t.Fatal(err)
	}

	var value string
	entries := make(map[string]*string)
	value = "3"
	entries["min.insync.replicas"] = &value

	for _, resourceType := range []ConfigResourceType{BrokerResource, BrokerLoggerResource} {
		resource := ConfigResource{Name: "2", Type: resourceType}
		err = admin.AlterConfig(
			resource.Type,
			resource.Name,
			entries,
			false)
		if err != nil {
			t.Fatal(err)
		}
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

	config := NewTestConfig()
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

	config := NewTestConfig()
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

	config := NewTestConfig()
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

	config := NewTestConfig()
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

	config := NewTestConfig()
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

	config := NewTestConfig()
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

	config := NewTestConfig()
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

	config := NewTestConfig()
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

	config := NewTestConfig()
	config.Version = V1_0_0_0

	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	response, err := admin.ListConsumerGroupOffsets(group, map[string][]int32{
		topic: {0},
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

	config := NewTestConfig()
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

// TestRefreshMetaDataWithDifferentController ensures that the cached
// controller can be forcibly updated from Metadata by the admin client
func TestRefreshMetaDataWithDifferentController(t *testing.T) {
	seedBroker1 := NewMockBroker(t, 1)
	seedBroker2 := NewMockBroker(t, 2)
	defer seedBroker1.Close()
	defer seedBroker2.Close()

	seedBroker1.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker1.BrokerID()).
			SetBroker(seedBroker1.Addr(), seedBroker1.BrokerID()).
			SetBroker(seedBroker2.Addr(), seedBroker2.BrokerID()),
	})

	config := NewTestConfig()
	config.Version = V1_1_0_0

	client, err := NewClient([]string{seedBroker1.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	ca := clusterAdmin{client: client, conf: config}

	if b, _ := ca.Controller(); seedBroker1.BrokerID() != b.ID() {
		t.Fatalf("expected cached controller to be %d rather than %d",
			seedBroker1.BrokerID(), b.ID())
	}

	seedBroker1.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker2.BrokerID()).
			SetBroker(seedBroker1.Addr(), seedBroker1.BrokerID()).
			SetBroker(seedBroker2.Addr(), seedBroker2.BrokerID()),
	})

	if b, _ := ca.refreshController(); seedBroker2.BrokerID() != b.ID() {
		t.Fatalf("expected refreshed controller to be %d rather than %d",
			seedBroker2.BrokerID(), b.ID())
	}

	if b, _ := ca.Controller(); seedBroker2.BrokerID() != b.ID() {
		t.Fatalf("expected cached controller to be %d rather than %d",
			seedBroker2.BrokerID(), b.ID())
	}
}

func TestDescribeLogDirs(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"DescribeLogDirsRequest": NewMockDescribeLogDirsResponse(t).
			SetLogDirs("/tmp/logs", map[string]int{"topic1": 2, "topic2": 2}),
	})

	config := NewTestConfig()
	config.Version = V1_0_0_0

	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	logDirsPerBroker, err := admin.DescribeLogDirs([]int32{seedBroker.BrokerID()})
	if err != nil {
		t.Fatal(err)
	}

	if len(logDirsPerBroker) != 1 {
		t.Fatalf("Expected %v results, got %v", 1, len(logDirsPerBroker))
	}
	logDirs := logDirsPerBroker[seedBroker.BrokerID()]
	if len(logDirs) != 1 {
		t.Fatalf("Expected log dirs for broker %v to be returned, but it did not, got %v", seedBroker.BrokerID(), len(logDirs))
	}
	logDirsBroker := logDirs[0]
	if logDirsBroker.ErrorCode != ErrNoError {
		t.Fatalf("Expected no error for broker %v, but it was %v", seedBroker.BrokerID(), logDirsBroker.ErrorCode)
	}
	if logDirsBroker.Path != "/tmp/logs" {
		t.Fatalf("Expected log dirs for broker %v to be '/tmp/logs', but it was %v", seedBroker.BrokerID(), logDirsBroker.Path)
	}
	if len(logDirsBroker.Topics) != 2 {
		t.Fatalf("Expected log dirs for broker %v to have 2 topics, but it had %v", seedBroker.BrokerID(), len(logDirsBroker.Topics))
	}
	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

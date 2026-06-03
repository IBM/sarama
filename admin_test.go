//go:build !functional

package sarama

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if admin != nil {
		defer safeClose(t, admin)
	}
	if err == nil {
		t.Fatal(errors.New("controller not set still cluster admin was created"))
	}

	if !errors.Is(err, ErrControllerNotAvailable) {
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

	if len(entries) == 0 {
		t.Fatal("no resource present")
	}

	topic, found := entries["my_topic"]
	if !found {
		t.Fatal("topic not found in response")
	}
	_, found = topic.ConfigEntries["max.message.bytes"]
	if found {
		t.Fatal("default topic config entry incorrectly found in response")
	}
	value := topic.ConfigEntries["retention.ms"]
	if value == nil || *value != "5000" {
		t.Fatal("non-default topic config entry not found in response")
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}

	assignment, found := topic.ReplicaAssignment[0]
	if !found {
		t.Fatal("replica assignment for partition 0 not found in response")
	}
	if len(assignment) == 0 {
		t.Fatal("replica assignment for partition 0 was empty")
	}
	if assignment[0] != 1 {
		t.Fatal("replica assignment not found in response")
	}
}

func TestClusterAdminListTopicsRetriesOnTransientConnectionError(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	metadataResponse := NewMockMetadataResponse(t).
		SetController(seedBroker.BrokerID()).
		SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
		SetLeader("my_topic", 0, seedBroker.BrokerID())

	var stateMu sync.Mutex
	var injectTimeout, injected bool
	var metadataAttempts int

	seedBroker.SetHandlerFuncByMap(map[string]requestHandlerFunc{
		"MetadataRequest": func(req *request) encoderWithHeader {
			stateMu.Lock()
			metadataAttempts++
			shouldInject := injectTimeout && !injected
			if shouldInject {
				injected = true
			}
			stateMu.Unlock()
			if shouldInject {
				return nil
			}
			return metadataResponse.For(req.body)
		},
		"DescribeConfigsRequest": func(req *request) encoderWithHeader {
			return NewMockDescribeConfigsResponse(t).For(req.body)
		},
	})

	config := NewTestConfig()
	config.Version = V1_1_0_0
	config.Net.ReadTimeout = 100 * time.Millisecond
	config.Admin.Retry.Max = 3
	config.Admin.Retry.Backoff = 10 * time.Millisecond

	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, admin)

	stateMu.Lock()
	injectTimeout = true
	metadataAttemptsBeforeList := metadataAttempts
	stateMu.Unlock()

	entries, err := admin.ListTopics()
	if err != nil {
		t.Fatal(err)
	}

	stateMu.Lock()
	gotInjected := injected
	gotMetadataAttempts := metadataAttempts
	stateMu.Unlock()
	if !gotInjected {
		t.Fatal("expected metadata timeout to be injected during ListTopics")
	}
	if gotMetadataAttempts < metadataAttemptsBeforeList+2 {
		t.Fatal("expected ListTopics to retry metadata after transient timeout")
	}
	if len(entries) == 0 {
		t.Fatal("no resource present")
	}
}

func TestClusterAdminListTopicsRetriesOnDescribeConfigsTimeout(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	metadataResponse := NewMockMetadataResponse(t).
		SetController(seedBroker.BrokerID()).
		SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
		SetLeader("my_topic", 0, seedBroker.BrokerID())

	var stateMu sync.Mutex
	var injectTimeout, injected bool
	var describeConfigsAttempts int
	describeConfigsResponse := NewMockDescribeConfigsResponse(t)

	seedBroker.SetHandlerFuncByMap(map[string]requestHandlerFunc{
		"MetadataRequest": func(req *request) encoderWithHeader {
			return metadataResponse.For(req.body)
		},
		"DescribeConfigsRequest": func(req *request) encoderWithHeader {
			stateMu.Lock()
			describeConfigsAttempts++
			shouldInject := injectTimeout && !injected
			if shouldInject {
				injected = true
			}
			stateMu.Unlock()
			if shouldInject {
				return nil
			}
			return describeConfigsResponse.For(req.body)
		},
	})

	config := NewTestConfig()
	config.Version = V1_1_0_0
	config.Net.ReadTimeout = 100 * time.Millisecond
	config.Admin.Retry.Max = 3
	config.Admin.Retry.Backoff = 10 * time.Millisecond

	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, admin)

	stateMu.Lock()
	injectTimeout = true
	describeConfigsAttemptsBeforeList := describeConfigsAttempts
	stateMu.Unlock()

	entries, err := admin.ListTopics()
	if err != nil {
		t.Fatal(err)
	}

	stateMu.Lock()
	gotInjected := injected
	gotDescribeConfigsAttempts := describeConfigsAttempts
	stateMu.Unlock()
	if !gotInjected {
		t.Fatal("expected DescribeConfigs timeout to be injected during ListTopics")
	}
	if gotDescribeConfigsAttempts < describeConfigsAttemptsBeforeList+2 {
		t.Fatal("expected ListTopics to retry DescribeConfigs after transient timeout")
	}
	if len(entries) == 0 {
		t.Fatal("no resource present")
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
	if !errors.Is(err, ErrInvalidTopic) {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminDeleteTopicError(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"DeleteTopicsRequest": NewMockDeleteTopicsResponse(t).SetError(ErrTopicDeletionDisabled),
	})

	config := NewTestConfig()
	config.Version = V0_10_2_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.DeleteTopic("my_topic")
	if !errors.Is(err, ErrTopicDeletionDisabled) {
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
	if !errors.Is(err, ErrUnsupportedVersion) {
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
		"ApiVersionsRequest": NewMockApiVersionsResponse(t),
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(secondBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.BrokerID()),
	})

	secondBroker.SetHandlerByMap(map[string]MockResponse{
		"ApiVersionsRequest":                 NewMockApiVersionsResponse(t),
		"AlterPartitionReassignmentsRequest": NewMockAlterPartitionReassignmentsResponse(t),
	})

	config := NewTestConfig()
	config.Version = V2_4_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	topicAssignment := make([][]int32, 0, 3)

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

	topicAssignment := make([][]int32, 0, 3)

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
		"ApiVersionsRequest": NewMockApiVersionsResponse(t),
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(secondBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.BrokerID()),
	})

	secondBroker.SetHandlerByMap(map[string]MockResponse{
		"ApiVersionsRequest":                NewMockApiVersionsResponse(t),
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
	}

	if len(partitionStatus) != 2 {
		t.Fatalf("partition missing in response")
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

	partitions := make([]int32, 0)

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

func TestClusterAdminDeleteRecordsWithUnsupportedVersion(t *testing.T) {
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
	if err == nil {
		t.Fatal("expected an ErrDeleteRecords")
	}

	if !strings.HasPrefix(err.Error(), "kafka server: failed to delete records") {
		t.Fatal(err)
	}

	if !errors.Is(err, ErrDeleteRecords) {
		t.Fatal(err)
	}

	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminDeleteRecordsWithLeaderNotAvailable(t *testing.T) {
	topicName := "my_topic"
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetLeader("my_topic", 1, -1).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
	})

	config := NewTestConfig()
	config.Version = V1_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	partitionOffset := make(map[int32]int64)
	partitionOffset[1] = 1000

	err = admin.DeleteRecords(topicName, partitionOffset)
	if err == nil {
		t.Fatal("expected an ErrDeleteRecords")
	}

	if !strings.HasPrefix(err.Error(), "kafka server: failed to delete records") {
		t.Fatal(err)
	}

	if !errors.Is(err, ErrDeleteRecords) {
		t.Fatal(err)
	}

	if !errors.Is(err, ErrLeaderNotAvailable) {
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

	tests := []struct {
		saramaVersion   KafkaVersion
		requestVersion  int16
		includeSynonyms bool
	}{
		{V1_0_0_0, 0, false},
		{V1_1_0_0, 1, true},
		{V1_1_1_0, 1, true},
		{V2_0_0_0, 2, true},
		{V2_6_0_0, 3, true},
		{V2_8_0_0, 4, true},
	}
	for _, tt := range tests {
		t.Run(tt.saramaVersion.String(), func(t *testing.T) {
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

			if len(entries) == 0 {
				t.Fatal(errors.New("no resource present"))
			}
			if tt.includeSynonyms {
				if len(entries[0].Synonyms) == 0 {
					t.Fatal("expected synonyms to have been included")
				}
			}
		})
	}
}

func TestClusterAdminDescribeConfigs(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"DescribeConfigsRequest": NewMockDescribeConfigsResponse(t),
	})

	config := NewTestConfig()
	config.Version = V2_8_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	resources := []*ConfigResource{
		{Name: "r1", Type: TopicResource, ConfigNames: []string{"my_topic"}},
		{Name: "r2", Type: TopicResource, ConfigNames: []string{"my_topic"}},
	}

	results, err := admin.DescribeConfigs(resources, DescribeConfigsOptions{
		IncludeSynonyms:      true,
		IncludeDocumentation: true,
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, "r1", results[0].Name)
	assert.Equal(t, "r2", results[1].Name)
	for _, result := range results {
		assert.Equal(t, ErrNoError, result.ErrorCode)
		assert.NotEmpty(t, result.Configs)
	}

	history := seedBroker.History()
	describeReq, ok := history[len(history)-1].Request.(*DescribeConfigsRequest)
	require.True(t, ok, "failed to find DescribeConfigsRequest in mockBroker history")
	assert.Equal(t, int16(4), describeReq.Version)
	assert.True(t, describeReq.IncludeSynonyms)
	assert.True(t, describeReq.IncludeDocumentation)
	assert.Len(t, describeReq.Resources, 2)
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

		if len(entries) == 0 {
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

func TestClusterAdminIncrementalAlterConfig(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"IncrementalAlterConfigsRequest": NewMockIncrementalAlterConfigsResponse(t),
	})

	config := NewTestConfig()
	config.Version = V2_3_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	var value string
	entries := make(map[string]IncrementalAlterConfigsEntry)
	value = "60000"
	entries["retention.ms"] = IncrementalAlterConfigsEntry{
		Operation: IncrementalAlterConfigsOperationSet,
		Value:     &value,
	}
	value = "1073741824"
	entries["segment.bytes"] = IncrementalAlterConfigsEntry{
		Operation: IncrementalAlterConfigsOperationDelete,
		Value:     &value,
	}
	err = admin.IncrementalAlterConfig(TopicResource, "my_topic", entries, false)
	if err != nil {
		t.Fatal(err)
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminIncrementalAlterConfigWithErrorCode(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"IncrementalAlterConfigsRequest": NewMockIncrementalAlterConfigsResponseWithErrorCode(t),
	})

	config := NewTestConfig()
	config.Version = V2_3_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = admin.Close()
	}()

	var value string
	entries := make(map[string]IncrementalAlterConfigsEntry)
	value = "60000"
	entries["retention.ms"] = IncrementalAlterConfigsEntry{
		Operation: IncrementalAlterConfigsOperationSet,
		Value:     &value,
	}
	value = "1073741824"
	entries["segment.bytes"] = IncrementalAlterConfigsEntry{
		Operation: IncrementalAlterConfigsOperationDelete,
		Value:     &value,
	}
	err = admin.IncrementalAlterConfig(TopicResource, "my_topic", entries, false)
	if err == nil {
		t.Fatal(errors.New("ErrorCode present but no Error returned"))
	}
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(errors.New("ErrorCode present but not wrapped into returned error"))
	}
}

func TestClusterAdminIncrementalAlterBrokerConfig(t *testing.T) {
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
		"IncrementalAlterConfigsRequest": NewMockIncrementalAlterConfigsResponse(t),
	})

	config := NewTestConfig()
	config.Version = V2_3_0_0
	admin, err := NewClusterAdmin(
		[]string{
			controllerBroker.Addr(),
			configBroker.Addr(),
		}, config)
	if err != nil {
		t.Fatal(err)
	}

	var value string
	entries := make(map[string]IncrementalAlterConfigsEntry)
	value = "3"
	entries["min.insync.replicas"] = IncrementalAlterConfigsEntry{
		Operation: IncrementalAlterConfigsOperationSet,
		Value:     &value,
	}
	value = "2"
	entries["log.cleaner.threads"] = IncrementalAlterConfigsEntry{
		Operation: IncrementalAlterConfigsOperationDelete,
		Value:     &value,
	}

	for _, resourceType := range []ConfigResourceType{BrokerResource, BrokerLoggerResource} {
		resource := ConfigResource{Name: "2", Type: resourceType}
		err = admin.IncrementalAlterConfig(
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

func TestClusterAdminCreateAclErrorHandling(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"CreateAclsRequest": NewMockCreateAclsResponseWithError(t),
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
	if err == nil {
		t.Fatal(errors.New("error should have been thrown"))
	}

	err = admin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterAdminCreateAcls(t *testing.T) {
	resource := Resource{ResourceType: AclResourceTopic, ResourceName: "my_topic"}
	acl := Acl{Host: "localhost", Operation: AclOperationAlter, PermissionType: AclPermissionAny}
	createOK := func(req *request) encoderWithHeader { return NewMockCreateAclsResponse(t).For(req.body) }
	notController := func(req *request) encoderWithHeader {
		r := req.body.(*CreateAclsRequest)
		rsp := &CreateAclsResponse{Version: r.version()}
		for range r.AclCreations {
			rsp.AclCreationResponses = append(rsp.AclCreationResponses, &AclCreationResponse{Err: ErrNotController})
		}
		return rsp
	}

	t.Run("creates acls", func(t *testing.T) {
		admin := singleBrokerAdmin(t, V1_0_0_0, map[string]requestHandlerFunc{
			"CreateAclsRequest": createOK,
		})

		rACLs := []*ResourceAcls{
			{
				Resource: resource,
				Acls:     []*Acl{&acl},
			},
			{
				Resource: Resource{ResourceType: AclResourceTopic, ResourceName: "your_topic"},
				Acls:     []*Acl{&acl},
			},
		}

		err := admin.CreateACLs(rACLs)
		require.NoError(t, err)
	})

	t.Run("retries on stale controller", func(t *testing.T) {
		admin, retriedOnNewController := staleControllerAdmin(t, V1_0_0_0, "CreateAclsRequest", notController, createOK)

		err := admin.CreateACL(resource, acl)
		require.NoError(t, err)
		assert.True(t, retriedOnNewController(), "expected broker 2 to receive the retried request")
	})

	t.Run("returns error when retries exhausted", func(t *testing.T) {
		admin, retriedOnNewController := staleControllerAdmin(t, V1_0_0_0, "CreateAclsRequest", notController, notController)

		err := admin.CreateACL(resource, acl)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNotController)
		assert.True(t, retriedOnNewController(), "expected broker 2 to receive the retried request")
	})
}

// singleBrokerAdmin wires a single mock broker that names itself controller,
// installs the given per-request handlers, and returns a ready admin client.
func singleBrokerAdmin(t *testing.T, version KafkaVersion, handlers map[string]requestHandlerFunc) ClusterAdmin {
	b := NewMockBroker(t, 1)
	t.Cleanup(func() { b.Close() })

	hm := map[string]requestHandlerFunc{
		"MetadataRequest": func(req *request) encoderWithHeader {
			return NewMockMetadataResponse(t).
				SetController(b.BrokerID()).
				SetBroker(b.Addr(), b.BrokerID()).For(req.body)
		},
	}
	for reqType, handler := range handlers {
		hm[reqType] = handler
	}
	b.SetHandlerFuncByMap(hm)

	config := NewTestConfig()
	config.Version = version
	admin, err := NewClusterAdmin([]string{b.Addr()}, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })

	return admin
}

// staleControllerAdmin wires two mock brokers into a controller-failover
// scenario and returns a ready admin client. Broker 1 answers reqType with
// errResp and flips fresh metadata to name broker 2 as controller, modeling a
// stale cached controller; broker 2 answers with okResp. retriedOnNewController
// reports whether broker 2 received the request.
func staleControllerAdmin(t *testing.T, version KafkaVersion, reqType string, errResp, okResp requestHandlerFunc) (admin ClusterAdmin, retriedOnNewController func() bool) {
	b1 := NewMockBroker(t, 1)
	b2 := NewMockBroker(t, 2)
	t.Cleanup(func() { b1.Close(); b2.Close() })

	var controllerID atomic.Int32
	controllerID.Store(b1.BrokerID())
	meta := func(req *request) encoderWithHeader {
		return NewMockMetadataResponse(t).SetController(controllerID.Load()).
			SetBroker(b1.Addr(), b1.BrokerID()).
			SetBroker(b2.Addr(), b2.BrokerID()).For(req.body)
	}

	var newControllerReceivedRequest atomic.Bool
	b1.SetHandlerFuncByMap(map[string]requestHandlerFunc{
		"MetadataRequest": meta,
		reqType: func(req *request) encoderWithHeader {
			controllerID.Store(b2.BrokerID()) // flip so the post-error refresh elects broker 2
			return errResp(req)
		},
	})
	b2.SetHandlerFuncByMap(map[string]requestHandlerFunc{
		"MetadataRequest": meta,
		reqType: func(req *request) encoderWithHeader {
			newControllerReceivedRequest.Store(true)
			return okResp(req)
		},
	})

	config := NewTestConfig()
	config.Version = version
	config.Admin.Retry.Backoff = time.Millisecond
	admin, err := NewClusterAdmin([]string{b1.Addr()}, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })

	return admin, newControllerReceivedRequest.Load
}

func TestClusterAdminListAcls(t *testing.T) {
	resourceName := "my_topic"
	filter := AclFilter{ResourceType: AclResourceTopic, Operation: AclOperationRead, ResourceName: &resourceName}
	listOK := func(req *request) encoderWithHeader { return NewMockListAclsResponse(t).For(req.body) }
	notController := func(req *request) encoderWithHeader {
		return &DescribeAclsResponse{Version: req.body.version(), Err: ErrNotController}
	}

	t.Run("lists acls", func(t *testing.T) {
		admin := singleBrokerAdmin(t, V1_0_0_0, map[string]requestHandlerFunc{
			"DescribeAclsRequest": listOK,
			"CreateAclsRequest":   func(req *request) encoderWithHeader { return NewMockCreateAclsResponse(t).For(req.body) },
		})

		err := admin.CreateACL(
			Resource{ResourceType: AclResourceTopic, ResourceName: "my_topic"},
			Acl{Host: "localhost", Operation: AclOperationAlter, PermissionType: AclPermissionAny},
		)
		require.NoError(t, err)

		acls, err := admin.ListAcls(filter)
		require.NoError(t, err)
		assert.NotEmpty(t, acls)
	})

	t.Run("retries on stale controller", func(t *testing.T) {
		admin, retriedOnNewController := staleControllerAdmin(t, V1_0_0_0, "DescribeAclsRequest", notController, listOK)

		acls, err := admin.ListAcls(filter)
		require.NoError(t, err)
		assert.NotEmpty(t, acls)
		assert.True(t, retriedOnNewController(), "expected broker 2 to receive the retried request")
	})

	t.Run("returns error when retries exhausted", func(t *testing.T) {
		admin, _ := staleControllerAdmin(t, V1_0_0_0, "DescribeAclsRequest", notController, notController)

		_, err := admin.ListAcls(filter)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNotController)
	})
}

func TestClusterAdminDeleteAcl(t *testing.T) {
	resourceName := "my_topic"
	filter := AclFilter{ResourceType: AclResourceTopic, Operation: AclOperationAlter, ResourceName: &resourceName}
	deleteOK := func(req *request) encoderWithHeader { return NewMockDeleteAclsResponse(t).For(req.body) }
	notController := func(req *request) encoderWithHeader {
		return &DeleteAclsResponse{
			Version:         req.body.version(),
			FilterResponses: []*FilterResponse{{Err: ErrNotController}},
		}
	}

	t.Run("deletes acls", func(t *testing.T) {
		admin := singleBrokerAdmin(t, V1_0_0_0, map[string]requestHandlerFunc{
			"DeleteAclsRequest": deleteOK,
		})

		_, err := admin.DeleteACL(filter, false)
		require.NoError(t, err)
	})

	t.Run("retries on stale controller", func(t *testing.T) {
		admin, retriedOnNewController := staleControllerAdmin(t, V1_0_0_0, "DeleteAclsRequest", notController, deleteOK)

		_, err := admin.DeleteACL(filter, false)
		require.NoError(t, err)
		assert.True(t, retriedOnNewController(), "expected broker 2 to receive the retried request")
	})

	t.Run("returns error when retries exhausted", func(t *testing.T) {
		admin, _ := staleControllerAdmin(t, V1_0_0_0, "DeleteAclsRequest", notController, notController)

		_, err := admin.DeleteACL(filter, false)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNotController)
	})
}

func TestClusterAdminDescribeUserScramCredentials(t *testing.T) {
	users := []string{"my_user"}
	describeOK := func(req *request) encoderWithHeader {
		return &DescribeUserScramCredentialsResponse{
			Version: req.body.version(),
			Results: []*DescribeUserScramCredentialsResult{{User: "my_user"}},
		}
	}
	notController := func(req *request) encoderWithHeader {
		return &DescribeUserScramCredentialsResponse{Version: req.body.version(), ErrorCode: ErrNotController}
	}

	t.Run("describes credentials", func(t *testing.T) {
		admin := singleBrokerAdmin(t, V2_7_0_0, map[string]requestHandlerFunc{
			"DescribeUserScramCredentialsRequest": describeOK,
		})

		results, err := admin.DescribeUserScramCredentials(users)
		require.NoError(t, err)
		assert.NotEmpty(t, results)
	})

	t.Run("retries on stale controller", func(t *testing.T) {
		admin, retriedOnNewController := staleControllerAdmin(t, V2_7_0_0, "DescribeUserScramCredentialsRequest", notController, describeOK)

		results, err := admin.DescribeUserScramCredentials(users)
		require.NoError(t, err)
		assert.NotEmpty(t, results)
		assert.True(t, retriedOnNewController(), "expected broker 2 to receive the retried request")
	})

	t.Run("returns error when retries exhausted", func(t *testing.T) {
		admin, _ := staleControllerAdmin(t, V2_7_0_0, "DescribeUserScramCredentialsRequest", notController, notController)

		_, err := admin.DescribeUserScramCredentials(users)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNotController)
	})
}

func TestElectLeaders(t *testing.T) {
	broker := NewMockBroker(t, 1)
	defer broker.Close()

	broker.SetHandlerByMap(map[string]MockResponse{
		"ApiVersionsRequest": NewMockApiVersionsResponse(t),
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(broker.BrokerID()).
			SetBroker(broker.Addr(), broker.BrokerID()),
		"ElectLeadersRequest": NewMockElectLeadersResponse(t),
	})

	config := NewTestConfig()
	config.Version = V2_4_0_0
	admin, err := NewClusterAdmin([]string{broker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	response, err := admin.ElectLeaders(PreferredElection, map[string][]int32{"my_topic": {0, 1}})
	if err != nil {
		t.Fatal(err)
	}

	partitionResult, ok := response["my_topic"]
	if !ok {
		t.Fatalf("topic missing in response")
	}

	if len(partitionResult) != 1 {
		t.Fatalf("partition missing in response")
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
	const (
		group          = "my-group"
		topic          = "my-topic"
		partition      = int32(0)
		expectedOffset = int64(0)
	)

	fetch := func(t *testing.T, version KafkaVersion) *OffsetFetchResponse {
		t.Helper()
		broker := newMockBroker(t, 1)
		broker.SetHandlerByMap(map[string]MockResponse{
			"OffsetFetchRequest": NewMockOffsetFetchResponse(t).
				SetOffset(group, topic, partition, expectedOffset, "", ErrNoError).
				SetError(ErrNoError),
			"MetadataRequest":        mockMetadataFor(t, broker),
			"FindCoordinatorRequest": mockGroupCoordinators(t, broker, group),
		})
		response, err := newTestAdminAt(t, version, broker).
			ListConsumerGroupOffsets(group, map[string][]int32{topic: {0}})
		require.NoError(t, err)
		return response
	}

	t.Run("returns legacy blocks for older versions", func(t *testing.T) {
		response := fetch(t, V1_0_0_0)
		block := response.GetBlock(topic, partition)
		require.NotNil(t, block)
		assert.Equal(t, expectedOffset, block.Offset)
		assert.Equal(t, expectedOffset, response.Blocks[topic][partition].Offset)
	})

	t.Run("mirrors v8 group response into legacy blocks", func(t *testing.T) {
		response := fetch(t, V3_0_0_0)
		block := response.GetBlock(topic, partition)
		require.NotNil(t, block)
		require.Contains(t, response.Blocks, topic)
		assert.Equal(t, expectedOffset, response.Blocks[topic][partition].Offset)
		assert.Equal(t, ErrNoError, response.Err)
	})
}

func TestListConsumerGroupOffsetsBatch(t *testing.T) {
	const (
		topic           = "my-topic"
		groupA          = "group-a"
		groupB          = "group-b"
		expectedOffsetA = int64(7)
		expectedOffsetB = int64(13)
	)
	bothGroups := map[string]map[string][]int32{
		groupA: {topic: {0}},
		groupB: {topic: {0}},
	}

	// setup wires a single mock broker as the coordinator for every named group
	// and serves the given OffsetFetchRequest handler. Returns a v3.0 admin.
	setup := func(t *testing.T, offsetFetch MockResponse, groups ...string) ClusterAdmin {
		t.Helper()
		broker := newMockBroker(t, 1)
		broker.SetHandlerByMap(map[string]MockResponse{
			"OffsetFetchRequest":     offsetFetch,
			"MetadataRequest":        mockMetadataFor(t, broker),
			"FindCoordinatorRequest": mockGroupCoordinators(t, broker, groups...),
		})
		return newTestAdminAt(t, V3_0_0_0, broker)
	}

	// groupBlock builds a v8 response group entry with a single block.
	groupBlock := func(group string, offset int64) OffsetFetchResponseGroup {
		return OffsetFetchResponseGroup{
			GroupId: group,
			Blocks:  map[string]map[int32]*OffsetFetchResponseBlock{topic: {0: {Offset: offset}}},
		}
	}

	t.Run("fetches offsets for multiple groups", func(t *testing.T) {
		admin := setup(t, NewMockOffsetFetchResponse(t).
			SetOffset(groupA, topic, 0, expectedOffsetA, "", ErrNoError).
			SetOffset(groupB, topic, 0, expectedOffsetB, "", ErrNoError).
			SetError(ErrNoError), groupA, groupB)

		result, err := admin.ListConsumerGroupOffsetsBatch(bothGroups)
		require.NoError(t, err)
		assertGroupOffset(t, result, groupA, topic, 0, expectedOffsetA)
		assertGroupOffset(t, result, groupB, topic, 0, expectedOffsetB)
	})

	t.Run("nil partitions fetches all topics for the group", func(t *testing.T) {
		const otherTopic = "other-topic"
		admin := setup(t, NewMockOffsetFetchResponse(t).
			SetOffset(groupA, topic, 0, expectedOffsetA, "", ErrNoError).
			SetOffset(groupA, otherTopic, 0, expectedOffsetB, "", ErrNoError).
			SetError(ErrNoError), groupA)

		result, err := admin.ListConsumerGroupOffsetsBatch(map[string]map[string][]int32{groupA: nil})
		require.NoError(t, err)
		assertGroupOffset(t, result, groupA, topic, 0, expectedOffsetA)
		assertGroupOffset(t, result, groupA, otherTopic, 0, expectedOffsetB)
	})

	t.Run("rejects broker downgrade below v8", func(t *testing.T) {
		broker := newMockBroker(t, 1)
		broker.SetHandlerByMap(map[string]MockResponse{
			"ApiVersionsRequest": NewMockApiVersionsResponse(t).SetApiKeys([]ApiVersionsResponseKey{
				{ApiKey: apiKeyOffsetFetch, MinVersion: 0, MaxVersion: 7},
			}),
			"MetadataRequest":        mockMetadataFor(t, broker),
			"FindCoordinatorRequest": mockGroupCoordinators(t, broker, groupA, groupB),
		})
		config := NewTestConfig()
		config.ApiVersionsRequest = true
		config.Version = V3_0_0_0
		admin, err := NewClusterAdmin([]string{broker.Addr()}, config)
		require.NoError(t, err)
		t.Cleanup(func() { admin.Close() })

		_, err = admin.ListConsumerGroupOffsetsBatch(bothGroups)
		require.ErrorIs(t, err, ErrUnsupportedVersion)
	})

	t.Run("limits broker's advertised max to client's supported max", func(t *testing.T) {
		// Kafka 4.x brokers advertise OffsetFetch versions above the highest the
		// client knows how to encode. The client must limit to its own max.
		// 9999 keeps this test honest as we add more protocol versions later.
		broker := newMockBroker(t, 1)
		broker.SetHandlerByMap(map[string]MockResponse{
			"ApiVersionsRequest": NewMockApiVersionsResponse(t).SetApiKeys([]ApiVersionsResponseKey{
				{ApiKey: apiKeyOffsetFetch, MinVersion: 0, MaxVersion: 9999},
			}),
			"MetadataRequest":        mockMetadataFor(t, broker),
			"FindCoordinatorRequest": mockGroupCoordinators(t, broker, groupA, groupB),
			"OffsetFetchRequest": NewMockOffsetFetchResponse(t).
				SetOffset(groupA, topic, 0, expectedOffsetA, "", ErrNoError).
				SetOffset(groupB, topic, 0, expectedOffsetB, "", ErrNoError).
				SetError(ErrNoError),
		})
		config := NewTestConfig()
		config.ApiVersionsRequest = true
		config.Version = V3_0_0_0
		admin, err := NewClusterAdmin([]string{broker.Addr()}, config)
		require.NoError(t, err)
		t.Cleanup(func() { admin.Close() })

		result, err := admin.ListConsumerGroupOffsetsBatch(bothGroups)
		require.NoError(t, err)
		assertGroupOffset(t, result, groupA, topic, 0, expectedOffsetA)
		assertGroupOffset(t, result, groupB, topic, 0, expectedOffsetB)
	})

	t.Run("retries on retriable per-group error", func(t *testing.T) {
		first := &OffsetFetchResponse{Version: 8, Groups: []OffsetFetchResponseGroup{
			{GroupId: groupA, Err: ErrNotCoordinatorForConsumer},
			groupBlock(groupB, expectedOffsetB),
		}}
		second := &OffsetFetchResponse{Version: 8, Groups: []OffsetFetchResponseGroup{
			groupBlock(groupA, expectedOffsetA),
			groupBlock(groupB, expectedOffsetB),
		}}
		admin := setup(t, NewMockSequence(first, second), groupA, groupB)

		result, err := admin.ListConsumerGroupOffsetsBatch(bothGroups)
		require.NoError(t, err)
		assertGroupOffset(t, result, groupA, topic, 0, expectedOffsetA)
		assertGroupOffset(t, result, groupB, topic, 0, expectedOffsetB)
	})

	t.Run("returns non-retriable per-group error without retry", func(t *testing.T) {
		resp := &OffsetFetchResponse{Version: 8, Groups: []OffsetFetchResponseGroup{
			{GroupId: groupA, Err: ErrGroupAuthorizationFailed},
			groupBlock(groupB, expectedOffsetB),
		}}
		admin := setup(t, NewMockWrapper(resp), groupA, groupB)

		result, err := admin.ListConsumerGroupOffsetsBatch(bothGroups)
		require.NoError(t, err)
		require.Contains(t, result, groupA)
		assert.Equal(t, ErrGroupAuthorizationFailed, result[groupA].Err)
		assertGroupOffset(t, result, groupB, topic, 0, expectedOffsetB)
	})

	t.Run("splits groups across coordinators", func(t *testing.T) {
		seed := newMockBroker(t, 1)
		coordA := newMockBroker(t, 2)
		coordB := newMockBroker(t, 3)
		metadata := mockMetadataFor(t, seed, coordA, coordB)
		findCoord := NewMockFindCoordinatorResponse(t).
			SetCoordinator(CoordinatorGroup, groupA, coordA).
			SetCoordinator(CoordinatorGroup, groupB, coordB)
		seed.SetHandlerByMap(map[string]MockResponse{
			"MetadataRequest":        metadata,
			"FindCoordinatorRequest": findCoord,
		})
		for _, c := range []struct {
			broker *MockBroker
			group  string
			offset int64
		}{
			{coordA, groupA, expectedOffsetA},
			{coordB, groupB, expectedOffsetB},
		} {
			c.broker.SetHandlerByMap(map[string]MockResponse{
				"MetadataRequest":        metadata,
				"FindCoordinatorRequest": findCoord,
				"OffsetFetchRequest": NewMockOffsetFetchResponse(t).
					SetOffset(c.group, topic, 0, c.offset, "", ErrNoError).
					SetError(ErrNoError),
			})
		}

		result, err := newTestAdminAt(t, V3_0_0_0, seed).ListConsumerGroupOffsetsBatch(bothGroups)
		require.NoError(t, err)
		assertGroupOffset(t, result, groupA, topic, 0, expectedOffsetA)
		assertGroupOffset(t, result, groupB, topic, 0, expectedOffsetB)
	})
}

// newMockBroker spins up a MockBroker with auto-cleanup.
func newMockBroker(t *testing.T, id int32) *MockBroker {
	t.Helper()
	b := NewMockBroker(t, id)
	t.Cleanup(func() { b.Close() })
	return b
}

// newTestAdmin builds a V2.1 ClusterAdmin against the given seed broker with
// retry backoff disabled and auto-cleanup.
func newTestAdmin(t *testing.T, seed *MockBroker) ClusterAdmin {
	t.Helper()
	return newTestAdminAt(t, V2_1_0_0, seed)
}

// newTestAdminAt is like newTestAdmin but with a caller-chosen KafkaVersion.
func newTestAdminAt(t *testing.T, version KafkaVersion, seed *MockBroker) ClusterAdmin {
	t.Helper()
	config := NewTestConfig()
	config.Version = version
	config.Admin.Retry.Backoff = 0
	admin, err := NewClusterAdmin([]string{seed.Addr()}, config)
	require.NoError(t, err)
	t.Cleanup(func() { admin.Close() })
	return admin
}

// mockGroupCoordinators builds a FindCoordinator handler placing every named
// group on coordinator.
func mockGroupCoordinators(t *testing.T, coordinator *MockBroker, groups ...string) *MockFindCoordinatorResponse {
	t.Helper()
	r := NewMockFindCoordinatorResponse(t)
	for _, g := range groups {
		r.SetCoordinator(CoordinatorGroup, g, coordinator)
	}
	return r
}

// assertGroupOffset asserts result has groupID with the expected offset at topic/partition.
func assertGroupOffset(t *testing.T, result map[string]*OffsetFetchResponseGroup, groupID, topic string, partition int32, expected int64) {
	t.Helper()
	require.Contains(t, result, groupID)
	block := result[groupID].GetBlock(topic, partition)
	require.NotNil(t, block)
	assert.Equal(t, expected, block.Offset)
}

// mockMetadataFor builds a MockMetadataResponse with controller and brokers
// populated. Callers chain .SetLeader as needed.
func mockMetadataFor(t *testing.T, controller *MockBroker, brokers ...*MockBroker) *MockMetadataResponse {
	t.Helper()
	m := NewMockMetadataResponse(t).
		SetController(controller.BrokerID()).
		SetBroker(controller.Addr(), controller.BrokerID())
	for _, b := range brokers {
		m.SetBroker(b.Addr(), b.BrokerID())
	}
	return m
}

func TestListOffsets(t *testing.T) {
	const topic = "my-topic"

	t.Run("returns offsets from a single broker", func(t *testing.T) {
		const partition = int32(0)
		const timestamp = int64(1690000000000)
		const expectedOffset = int64(42)

		broker := newMockBroker(t, 1)
		broker.SetHandlerByMap(map[string]MockResponse{
			"OffsetRequest":   NewMockOffsetResponse(t).SetOffset(topic, partition, timestamp, expectedOffset),
			"MetadataRequest": mockMetadataFor(t, broker).SetLeader(topic, partition, broker.BrokerID()),
		})

		result, err := newTestAdmin(t, broker).ListOffsets(map[string]map[int32]int64{
			topic: {partition: timestamp},
		}, nil)
		require.NoError(t, err)

		info := result[topic][partition]
		require.NotNil(t, info)
		assert.Equal(t, ErrNoError, info.Err)
		assert.Equal(t, expectedOffset, info.Offset)
	})

	t.Run("fans out across partition leaders", func(t *testing.T) {
		const offset1 = int64(100)
		const offset2 = int64(200)

		leader1 := newMockBroker(t, 1)
		leader2 := newMockBroker(t, 2)
		metadata := mockMetadataFor(t, leader1, leader2).
			SetLeader(topic, 0, leader1.BrokerID()).
			SetLeader(topic, 1, leader2.BrokerID())

		leader1.SetHandlerByMap(map[string]MockResponse{
			"MetadataRequest": metadata,
			"OffsetRequest":   NewMockOffsetResponse(t).SetOffset(topic, 0, OffsetNewest, offset1),
		})
		leader2.SetHandlerByMap(map[string]MockResponse{
			"MetadataRequest": metadata,
			"OffsetRequest":   NewMockOffsetResponse(t).SetOffset(topic, 1, OffsetNewest, offset2),
		})

		result, err := newTestAdmin(t, leader1).ListOffsets(map[string]map[int32]int64{
			topic: {0: OffsetNewest, 1: OffsetNewest},
		}, nil)
		require.NoError(t, err)
		require.Contains(t, result, topic)
		assert.Equal(t, offset1, result[topic][0].Offset)
		assert.Equal(t, offset2, result[topic][1].Offset)
	})

	t.Run("propagates IsolationLevel to the broker request", func(t *testing.T) {
		const partition = int32(0)

		broker := newMockBroker(t, 1)
		metadata := mockMetadataFor(t, broker).SetLeader(topic, partition, broker.BrokerID())
		offsets := NewMockOffsetResponse(t).SetOffset(topic, partition, OffsetNewest, 42)

		var captured atomic.Int32
		captured.Store(-1)
		broker.SetHandlerFuncByMap(map[string]requestHandlerFunc{
			"MetadataRequest": func(r *request) encoderWithHeader { return metadata.For(r.body) },
			"OffsetRequest": func(r *request) encoderWithHeader {
				captured.Store(int32(r.body.(*OffsetRequest).IsolationLevel))
				return offsets.For(r.body)
			},
		})

		_, err := newTestAdmin(t, broker).ListOffsets(map[string]map[int32]int64{
			topic: {partition: OffsetNewest},
		}, &ListOffsetsOptions{IsolationLevel: ReadCommitted})
		require.NoError(t, err)
		assert.Equal(t, int32(ReadCommitted), captured.Load())
	})

	t.Run("returns ConfigurationError when no partitions provided", func(t *testing.T) {
		broker := newMockBroker(t, 1)
		broker.SetHandlerByMap(map[string]MockResponse{"MetadataRequest": mockMetadataFor(t, broker)})

		result, err := newTestAdmin(t, broker).ListOffsets(nil, nil)
		assert.Nil(t, result)

		var cfgErr ConfigurationError
		require.ErrorAs(t, err, &cfgErr)
	})

	t.Run("records metadata-resolve failure as a per-partition error", func(t *testing.T) {
		const knownPartition = int32(0)
		const missingPartition = int32(1)
		const expectedOffset = int64(7)

		broker := newMockBroker(t, 1)
		broker.SetHandlerByMap(map[string]MockResponse{
			"OffsetRequest":   NewMockOffsetResponse(t).SetOffset(topic, knownPartition, OffsetNewest, expectedOffset),
			"MetadataRequest": mockMetadataFor(t, broker).SetLeader(topic, knownPartition, broker.BrokerID()),
		})

		result, err := newTestAdmin(t, broker).ListOffsets(map[string]map[int32]int64{
			topic: {knownPartition: OffsetNewest, missingPartition: OffsetNewest},
		}, nil)
		require.NoError(t, err)

		known := result[topic][knownPartition]
		require.NotNil(t, known)
		assert.Equal(t, ErrNoError, known.Err)
		assert.Equal(t, expectedOffset, known.Offset)

		missing := result[topic][missingPartition]
		require.NotNil(t, missing)
		assert.ErrorIs(t, missing.Err, ErrUnknownTopicOrPartition)
	})
}

func TestAlterConsumerGroupOffsets(t *testing.T) {
	const (
		group     = "my-group"
		topic     = "my-topic"
		partition = int32(0)
	)
	offsets := map[string]map[int32]OffsetAndMetadata{
		topic: {partition: {Offset: 100, LeaderEpoch: -1}},
	}

	t.Run("commits offsets via the group coordinator", func(t *testing.T) {
		broker := newMockBroker(t, 1)
		broker.SetHandlerByMap(map[string]MockResponse{
			"OffsetCommitRequest":    NewMockOffsetCommitResponse(t).SetError(group, topic, partition, ErrNoError),
			"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).SetCoordinator(CoordinatorGroup, group, broker),
			"MetadataRequest":        mockMetadataFor(t, broker),
		})

		response, err := newTestAdmin(t, broker).AlterConsumerGroupOffsets(group, offsets, nil)
		require.NoError(t, err)
		assert.Equal(t, ErrNoError, response.Errors[topic][partition])
	})

	t.Run("returns ConfigurationError when no offsets provided", func(t *testing.T) {
		broker := newMockBroker(t, 1)
		broker.SetHandlerByMap(map[string]MockResponse{"MetadataRequest": mockMetadataFor(t, broker)})

		response, err := newTestAdmin(t, broker).AlterConsumerGroupOffsets(group, nil, nil)
		assert.Nil(t, response)

		var cfgErr ConfigurationError
		require.ErrorAs(t, err, &cfgErr)
	})

	t.Run("retries on per-partition NOT_COORDINATOR", func(t *testing.T) {
		broker := newMockBroker(t, 1)
		broker.SetHandlerByMap(map[string]MockResponse{
			"OffsetCommitRequest": NewMockSequence(
				NewMockOffsetCommitResponse(t).SetError(group, topic, partition, ErrNotCoordinatorForConsumer),
				NewMockOffsetCommitResponse(t).SetError(group, topic, partition, ErrNoError),
			),
			"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).SetCoordinator(CoordinatorGroup, group, broker),
			"MetadataRequest":        mockMetadataFor(t, broker),
		})

		response, err := newTestAdmin(t, broker).AlterConsumerGroupOffsets(group, offsets, nil)
		require.NoError(t, err)
		assert.Equal(t, ErrNoError, response.Errors[topic][partition])
	})
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
	defer admin.Close()

	err = admin.DeleteConsumerGroup(group)
	if err != nil {
		t.Fatalf("DeleteConsumerGroup failed with error %v", err)
	}
}

func TestDeleteOffset(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	group := "group-delete-offset"
	topic := "topic-delete-offset"
	partition := int32(0)

	handlerMap := map[string]MockResponse{
		"ApiVersionsRequest": NewMockApiVersionsResponse(t),
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).SetCoordinator(CoordinatorGroup, group, seedBroker),
	}
	seedBroker.SetHandlerByMap(handlerMap)

	config := NewTestConfig()
	config.Version = V2_4_0_0

	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	// Test NoError
	handlerMap["DeleteOffsetsRequest"] = NewMockDeleteOffsetRequest(t).SetDeletedOffset(ErrNoError, topic, partition, ErrNoError)
	seedBroker.SetHandlerByMap(handlerMap)
	err = admin.DeleteConsumerGroupOffset(group, topic, partition)
	if err != nil {
		t.Fatalf("DeleteConsumerGroupOffset failed with error %v", err)
	}
	defer admin.Close()

	// Test Error
	handlerMap["DeleteOffsetsRequest"] = NewMockDeleteOffsetRequest(t).SetDeletedOffset(ErrNotCoordinatorForConsumer, topic, partition, ErrNoError)
	seedBroker.SetHandlerByMap(handlerMap)
	err = admin.DeleteConsumerGroupOffset(group, topic, partition)
	if !errors.Is(err, ErrNotCoordinatorForConsumer) {
		t.Fatalf("DeleteConsumerGroupOffset should have failed with error %v", ErrNotCoordinatorForConsumer)
	}

	// Test Error for partition
	handlerMap["DeleteOffsetsRequest"] = NewMockDeleteOffsetRequest(t).SetDeletedOffset(ErrNoError, topic, partition, ErrGroupSubscribedToTopic)
	seedBroker.SetHandlerByMap(handlerMap)
	err = admin.DeleteConsumerGroupOffset(group, topic, partition)
	if !errors.Is(err, ErrGroupSubscribedToTopic) {
		t.Fatalf("DeleteConsumerGroupOffset should have failed with error %v", ErrGroupSubscribedToTopic)
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
	defer client.Close()

	ca := clusterAdmin{client: client, conf: config}

	if b, _ := ca.Controller(); seedBroker1.BrokerID() != b.ID() {
		t.Fatalf("expected cached controller to be %d rather than %d",
			seedBroker1.BrokerID(), b.ID())
	}

	metadataResponse := NewMockMetadataResponse(t).
		SetController(seedBroker2.BrokerID()).
		SetBroker(seedBroker1.Addr(), seedBroker1.BrokerID()).
		SetBroker(seedBroker2.Addr(), seedBroker2.BrokerID())
	seedBroker1.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": metadataResponse,
	})
	seedBroker2.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": metadataResponse,
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
	if !errors.Is(logDirsBroker.ErrorCode, ErrNoError) {
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

func TestDescribeLogDirsUnknownBroker(t *testing.T) {
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
	defer safeClose(t, admin)

	type result struct {
		metadata map[int32][]DescribeLogDirsResponseDirMetadata
		err      error
	}

	res := make(chan result)

	go func() {
		metadata, err := admin.DescribeLogDirs([]int32{seedBroker.BrokerID() + 1})
		res <- result{metadata, err}
	}()

	select {
	case <-time.After(time.Second):
		t.Fatalf("DescribeLogDirs timed out")
	case returned := <-res:
		if len(returned.metadata) != 0 {
			t.Fatalf("Expected no results, got %v", len(returned.metadata))
		}
		if returned.err != nil {
			t.Fatalf("Expected no error, got %v", returned.err)
		}
	}
}

func Test_retryOnError(t *testing.T) {
	testBackoffTime := 100 * time.Millisecond
	config := NewTestConfig()
	config.Version = V1_0_0_0
	config.Admin.Retry.Max = 3
	config.Admin.Retry.Backoff = testBackoffTime

	admin := &clusterAdmin{conf: config}

	t.Run("immediate success", func(t *testing.T) {
		startTime := time.Now()
		attempts := 0
		err := admin.retryOnError(
			func(error) bool { return true },
			func() error {
				attempts++
				return nil
			})
		if err != nil {
			t.Fatalf("expected no error but was %v", err)
		}
		if attempts != 1 {
			t.Fatalf("expected 1 attempt to have been made but was %d", attempts)
		}
		if time.Since(startTime) >= testBackoffTime {
			t.Fatalf("single attempt should take less than backoff time")
		}
	})

	t.Run("immediate failure", func(t *testing.T) {
		startTime := time.Now()
		attempts := 0
		err := admin.retryOnError(
			func(error) bool { return false },
			func() error {
				attempts++
				return errors.New("mock error")
			})
		if err == nil {
			t.Fatalf("expected error but was nil")
		}
		if attempts != 1 {
			t.Fatalf("expected 1 attempt to have been made but was %d", attempts)
		}
		if time.Since(startTime) >= testBackoffTime {
			t.Fatalf("single attempt should take less than backoff time")
		}
	})

	t.Run("failing all attempts", func(t *testing.T) {
		startTime := time.Now()
		attempts := 0
		err := admin.retryOnError(
			func(error) bool { return true },
			func() error {
				attempts++
				return errors.New("mock error")
			})
		if err == nil {
			t.Errorf("expected error but was nil")
		}
		if attempts != 4 {
			t.Errorf("expected 4 attempts to have been made but was %d", attempts)
		}
		if time.Since(startTime) >= 4*testBackoffTime {
			t.Errorf("attempt+sleep+retry+sleep+retry+sleep+retry should take less than 4 * backoff time")
		}
	})
}

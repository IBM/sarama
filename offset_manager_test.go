package sarama

import (
	"log"
	"testing"
	"time"
)

var (
	broker, coordinator *mockBroker
	testClient          Client
)

func TestNewOffsetManager(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	seedBroker.Returns(new(MetadataResponse))

	testClient, err := NewClient([]string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewOffsetManagerFromClient("grouop", testClient)
	if err != nil {
		t.Error(err)
	}

	testClient.Close()

	_, err = NewOffsetManagerFromClient("group", testClient)
	if err != ErrClosedClient {
		t.Errorf("Error expected for closed client; actual value: %v", err)
	}

	seedBroker.Close()
}

func TestFetchInitialFail(t *testing.T) {
	// Mostly copy-paste from initPOM
	// TODO: eliminate this repetition
	config := NewConfig()
	config.Metadata.Retry.Max = 0
	config.Consumer.Offsets.CommitInterval = 1 * time.Millisecond

	broker = newMockBroker(t, 1)
	coordinator = newMockBroker(t, 2)

	seedMeta := new(MetadataResponse)
	seedMeta.AddBroker(coordinator.Addr(), coordinator.BrokerID())
	seedMeta.AddTopicPartition("my_topic", 0, 1, []int32{}, []int32{}, ErrNoError)
	seedMeta.AddTopicPartition("my_topic", 1, 1, []int32{}, []int32{}, ErrNoError)
	broker.Returns(seedMeta)

	var err error
	testClient, err = NewClient([]string{broker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	broker.Returns(&ConsumerMetadataResponse{
		CoordinatorID:   coordinator.BrokerID(),
		CoordinatorHost: "127.0.0.1",
		CoordinatorPort: coordinator.Port(),
	})

	om, err := NewOffsetManagerFromClient("group", testClient)
	if err != nil {
		t.Fatal(err)
	}

	fetchResponse := new(OffsetFetchResponse)

	// Only Err below modified
	fetchResponse.AddBlock("my_topic", 0, &OffsetFetchResponseBlock{
		Err:      ErrNotCoordinatorForConsumer,
		Offset:   5,
		Metadata: "test_meta",
	})
	coordinator.Returns(fetchResponse)

	_, err = om.ManagePartition("my_topic", 0)
	if err != ErrNotCoordinatorForConsumer {
		t.Error(err)
	}

}

func initPOM(t *testing.T) PartitionOffsetManager {
	config := NewConfig()
	config.Metadata.Retry.Max = 0
	config.Consumer.Offsets.CommitInterval = 1 * time.Millisecond

	broker = newMockBroker(t, 1)
	coordinator = newMockBroker(t, 2)

	seedMeta := new(MetadataResponse)
	seedMeta.AddBroker(coordinator.Addr(), coordinator.BrokerID())
	seedMeta.AddTopicPartition("my_topic", 0, 1, []int32{}, []int32{}, ErrNoError)
	seedMeta.AddTopicPartition("my_topic", 1, 1, []int32{}, []int32{}, ErrNoError)
	broker.Returns(seedMeta)

	var err error
	testClient, err = NewClient([]string{broker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	broker.Returns(&ConsumerMetadataResponse{
		CoordinatorID:   coordinator.BrokerID(),
		CoordinatorHost: "127.0.0.1",
		CoordinatorPort: coordinator.Port(),
	})

	om, err := NewOffsetManagerFromClient("group", testClient)
	if err != nil {
		t.Fatal(err)
	}
	fetchResponse := new(OffsetFetchResponse)
	fetchResponse.AddBlock("my_topic", 0, &OffsetFetchResponseBlock{
		Err:      ErrNoError,
		Offset:   5,
		Metadata: "test_meta",
	})
	coordinator.Returns(fetchResponse)

	pom, err := om.ManagePartition("my_topic", 0)
	if err != nil {
		t.Fatal(err)
	}
	return pom
}

func TestOffset(t *testing.T) {
	pom := initPOM(t)
	offset, meta := pom.Offset()
	if offset != 5 {
		t.Errorf("Expected offset 5. Actual: %v", offset)
	}
	if meta != "test_meta" {
		t.Errorf("Expected metadata \"test_meta\". Actual: %q", meta)
	}

	pom.Close()
	testClient.Close()
	broker.Close()
	coordinator.Close()
}

func TestSetOffset(t *testing.T) {
	pom := initPOM(t)

	ocResponse := new(OffsetCommitResponse)
	ocResponse.AddError("my_topic", 0, ErrNoError)
	coordinator.Returns(ocResponse)

	pom.SetOffset(100, "modified_meta")
	offset, meta := pom.Offset()

	if offset != 100 {
		t.Errorf("Expected offset 100. Actual: %v", offset)
	}
	if meta != "modified_meta" {
		t.Errorf("Expected metadata \"modified_meta\". Actual: %q", meta)
	}

	pom.Close()
	testClient.Close()
	broker.Close()
	coordinator.Close()
}

// This test is not passing
func TestCommitErr(t *testing.T) {
	log.Println("TestCommitErr")
	log.Println("=============")
	pom := initPOM(t)

	ocResponse := new(OffsetCommitResponse)
	// ocResponse.Errors
	ocResponse.AddError("my_topic", 0, ErrOffsetOutOfRange)
	ocResponse.AddError("my_topic", 1, ErrNoError)
	coordinator.Returns(ocResponse)

	// ocResponse2 := new(OffsetCommitResponse)
	// // ocResponse.Errors
	// ocResponse2.AddError("my_topic", 1, ErrNoError)
	// coordinator.Returns(ocResponse2)

	freshCoordinator := newMockBroker(t, 3)

	// For RefreshCoordinator()
	coordinatorResponse3 := new(ConsumerMetadataResponse)
	coordinatorResponse3.CoordinatorID = freshCoordinator.BrokerID()
	coordinatorResponse3.CoordinatorHost = "127.0.0.1"
	coordinatorResponse3.CoordinatorPort = freshCoordinator.Port()
	broker.Returns(coordinatorResponse3)

	ocResponse2 := new(OffsetCommitResponse)
	// ocResponse.Errors
	// ocResponse2.AddError("my_topic", 0, ErrOffsetOutOfRange)
	ocResponse2.AddError("my_topic", 0, ErrNoError)
	freshCoordinator.Returns(ocResponse2)

	pom.SetOffset(100, "modified_meta")

	err := pom.Close()
	if err != nil {
		t.Error(err)
	}
}

package sarama

import (
	"testing"
	"time"
)

var (
	broker, coordinator *mockBroker
	testClient          Client
	config              *Config
)

func initOM(t *testing.T) OffsetManager {
	config = NewConfig()
	config.Metadata.Retry.Max = 1
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

	return om
}

func initPOM(t *testing.T) PartitionOffsetManager {
	om := initOM(t)

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

func TestNewOffsetManager(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	seedBroker.Returns(new(MetadataResponse))

	testClient, err := NewClient([]string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewOffsetManagerFromClient("group", testClient)
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

// Test recovery from ErrNotCoordinatorForConsumer
// on first fetchInitialOffset call
func TestFetchInitialFail(t *testing.T) {
	om := initOM(t)

	// Error on first fetchInitialOffset call
	responseBlock := OffsetFetchResponseBlock{
		Err:      ErrNotCoordinatorForConsumer,
		Offset:   5,
		Metadata: "test_meta",
	}

	fetchResponse := new(OffsetFetchResponse)
	fetchResponse.AddBlock("my_topic", 0, &responseBlock)
	coordinator.Returns(fetchResponse)

	// Refresh coordinator
	newCoordinator := newMockBroker(t, 3)
	broker.Returns(&ConsumerMetadataResponse{
		CoordinatorID:   newCoordinator.BrokerID(),
		CoordinatorHost: "127.0.0.1",
		CoordinatorPort: newCoordinator.Port(),
	})

	// Second fetchInitialOffset call is fine
	fetchResponse2 := new(OffsetFetchResponse)
	responseBlock2 := responseBlock
	responseBlock2.Err = ErrNoError
	fetchResponse2.AddBlock("my_topic", 0, &responseBlock2)
	newCoordinator.Returns(fetchResponse2)

	pom, err := om.ManagePartition("my_topic", 0)
	if err != nil {
		t.Error(err)
	}

	broker.Close()
	coordinator.Close()
	newCoordinator.Close()
	pom.Close()
	testClient.Close()

	// Wait to see what the brokerOffsetManager does
	time.Sleep(500 * time.Millisecond)
}

// Test fetchInitialOffset retry on ErrOffsetsLoadInProgress
func TestFetchInitialInProgress(t *testing.T) {
	om := initOM(t)

	// Error on first fetchInitialOffset call
	responseBlock := OffsetFetchResponseBlock{
		Err:      ErrOffsetsLoadInProgress,
		Offset:   5,
		Metadata: "test_meta",
	}

	fetchResponse := new(OffsetFetchResponse)
	fetchResponse.AddBlock("my_topic", 0, &responseBlock)
	coordinator.Returns(fetchResponse)

	// Second fetchInitialOffset call is fine
	fetchResponse2 := new(OffsetFetchResponse)
	responseBlock2 := responseBlock
	responseBlock2.Err = ErrNoError
	fetchResponse2.AddBlock("my_topic", 0, &responseBlock2)
	coordinator.Returns(fetchResponse2)

	pom, err := om.ManagePartition("my_topic", 0)
	if err != nil {
		t.Error(err)
	}

	broker.Close()
	coordinator.Close()
	pom.Close()
	testClient.Close()
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
	broker.Close()
	coordinator.Close()
	testClient.Close()
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

func TestCommitErr(t *testing.T) {
	pom := initPOM(t)

	// Error on one partition
	ocResponse := new(OffsetCommitResponse)
	ocResponse.AddError("my_topic", 0, ErrOffsetOutOfRange)
	ocResponse.AddError("my_topic", 1, ErrNoError)
	coordinator.Returns(ocResponse)

	newCoordinator := newMockBroker(t, 3)

	// For RefreshCoordinator()
	broker.Returns(&ConsumerMetadataResponse{
		CoordinatorID:   newCoordinator.BrokerID(),
		CoordinatorHost: "127.0.0.1",
		CoordinatorPort: newCoordinator.Port(),
	})

	// Nothing in response.Errors at all
	ocResponse2 := new(OffsetCommitResponse)
	newCoordinator.Returns(ocResponse2)

	// For RefreshCoordinator()
	broker.Returns(&ConsumerMetadataResponse{
		CoordinatorID:   newCoordinator.BrokerID(),
		CoordinatorHost: "127.0.0.1",
		CoordinatorPort: newCoordinator.Port(),
	})

	// Error on the wrong partition for this pom
	ocResponse3 := new(OffsetCommitResponse)
	ocResponse3.AddError("my_topic", 1, ErrNoError)
	newCoordinator.Returns(ocResponse3)

	// For RefreshCoordinator()
	broker.Returns(&ConsumerMetadataResponse{
		CoordinatorID:   newCoordinator.BrokerID(),
		CoordinatorHost: "127.0.0.1",
		CoordinatorPort: newCoordinator.Port(),
	})

	// ErrUnknownTopicOrPartition/ErrNotLeaderForPartition/ErrLeaderNotAvailable block
	ocResponse4 := new(OffsetCommitResponse)
	ocResponse4.AddError("my_topic", 0, ErrUnknownTopicOrPartition)
	newCoordinator.Returns(ocResponse4)

	// For RefreshCoordinator()
	broker.Returns(&ConsumerMetadataResponse{
		CoordinatorID:   newCoordinator.BrokerID(),
		CoordinatorHost: "127.0.0.1",
		CoordinatorPort: newCoordinator.Port(),
	})

	// Normal error response
	ocResponse5 := new(OffsetCommitResponse)
	ocResponse5.AddError("my_topic", 0, ErrNoError)
	newCoordinator.Returns(ocResponse5)

	pom.SetOffset(100, "modified_meta")

	err := pom.Close()
	if err != nil {
		t.Error(err)
	}

	broker.Close()
	coordinator.Close()
	newCoordinator.Close()
	testClient.Close()
}

// Test of recovery from abort
func TestBOMAbort(t *testing.T) {
	pom := initPOM(t)

	// this triggers an error in the CommitOffset request,
	// which leads to the abort call
	coordinator.Close()

	// Response to refresh coordinator request
	newCoordinator := newMockBroker(t, 3)
	broker.Returns(&ConsumerMetadataResponse{
		CoordinatorID:   newCoordinator.BrokerID(),
		CoordinatorHost: "127.0.0.1",
		CoordinatorPort: newCoordinator.Port(),
	})

	ocResponse := new(OffsetCommitResponse)
	ocResponse.AddError("my_topic", 0, ErrNoError)
	newCoordinator.Returns(ocResponse)

	pom.SetOffset(100, "modified_meta")

	pom.Close()
	broker.Close()
	testClient.Close()
}

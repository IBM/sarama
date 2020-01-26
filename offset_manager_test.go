package sarama

import (
	"sync/atomic"
	"testing"
	"time"
)

func initOffsetManagerWithBackoffFunc(t *testing.T, retention time.Duration,
	backoffFunc func(retries, maxRetries int) time.Duration, config *Config) (om OffsetManager,
	testClient Client, broker, coordinator *MockBroker) {
	config.Metadata.Retry.Max = 1
	if backoffFunc != nil {
		config.Metadata.Retry.BackoffFunc = backoffFunc
	}
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Millisecond
	config.Version = V0_9_0_0
	if retention > 0 {
		config.Consumer.Offsets.Retention = retention
	}

	broker = NewMockBroker(t, 1)
	coordinator = NewMockBroker(t, 2)

	seedMeta := new(MetadataResponse)
	seedMeta.AddBroker(coordinator.Addr(), coordinator.BrokerID())
	seedMeta.AddTopicPartition("my_topic", 0, 1, []int32{}, []int32{}, []int32{}, ErrNoError)
	seedMeta.AddTopicPartition("my_topic", 1, 1, []int32{}, []int32{}, []int32{}, ErrNoError)
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

	om, err = NewOffsetManagerFromClient("group", testClient)
	if err != nil {
		t.Fatal(err)
	}

	return om, testClient, broker, coordinator
}

func initOffsetManager(t *testing.T, retention time.Duration) (om OffsetManager,
	testClient Client, broker, coordinator *MockBroker) {
	return initOffsetManagerWithBackoffFunc(t, retention, nil, NewConfig())
}

func initPartitionOffsetManager(t *testing.T, om OffsetManager,
	coordinator *MockBroker, initialOffset int64, metadata string) PartitionOffsetManager {
	fetchResponse := new(OffsetFetchResponse)
	fetchResponse.AddBlock("my_topic", 0, &OffsetFetchResponseBlock{
		Err:      ErrNoError,
		Offset:   initialOffset,
		Metadata: metadata,
	})
	coordinator.Returns(fetchResponse)

	pom, err := om.ManagePartition("my_topic", 0)
	if err != nil {
		t.Fatal(err)
	}

	return pom
}

func TestNewOffsetManager(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	seedBroker.Returns(new(MetadataResponse))
	defer seedBroker.Close()

	testClient, err := NewClient([]string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	om, err := NewOffsetManagerFromClient("group", testClient)
	if err != nil {
		t.Error(err)
	}
	safeClose(t, om)
	safeClose(t, testClient)

	_, err = NewOffsetManagerFromClient("group", testClient)
	if err != ErrClosedClient {
		t.Errorf("Error expected for closed client; actual value: %v", err)
	}
}

var offsetsautocommitTestTable = []struct {
	name   string
	set    bool // if given will override default configuration for Consumer.Offsets.AutoCommit.Enable
	enable bool
}{
	{
		"AutoCommit (default)",
		false, // use default
		true,
	},
	{
		"AutoCommit Enabled",
		true,
		true,
	},
	{
		"AutoCommit Disabled",
		true,
		false,
	},
}

func TestNewOffsetManagerOffsetsAutoCommit(t *testing.T) {
	// Tests to validate configuration of `Consumer.Offsets.AutoCommit.Enable`
	for _, tt := range offsetsautocommitTestTable {
		t.Run(tt.name, func(t *testing.T) {
			config := NewConfig()
			if tt.set {
				config.Consumer.Offsets.AutoCommit.Enable = tt.enable
			}
			om, testClient, broker, coordinator := initOffsetManagerWithBackoffFunc(t, 0, nil, config)
			pom := initPartitionOffsetManager(t, om, coordinator, 5, "original_meta")

			// Wait long enough for the test not to fail..
			timeout := 50 * config.Consumer.Offsets.AutoCommit.Interval

			called := make(chan none)

			ocResponse := new(OffsetCommitResponse)
			ocResponse.AddError("my_topic", 0, ErrNoError)
			handler := func(req *request) (res encoder) {
				close(called)
				return ocResponse
			}
			coordinator.setHandler(handler)

			// Should force an offset commit, if auto-commit is enabled.
			expected := int64(1)
			pom.ResetOffset(expected, "modified_meta")
			_, _ = pom.NextOffset()

			select {
			case <-called:
				// OffsetManager called on the wire.
				if !config.Consumer.Offsets.AutoCommit.Enable {
					t.Errorf("Received request for: %s when AutoCommit is disabled", tt.name)
				}
			case <-time.After(timeout):
				// Timeout waiting for OffsetManager to call on the wire.
				if config.Consumer.Offsets.AutoCommit.Enable {
					t.Errorf("No request received for: %s after waiting for %v", tt.name, timeout)
				}
			}

			broker.Close()
			coordinator.Close()

			// !! om must be closed before the pom so pom.release() is called before pom.Close()
			safeClose(t, om)
			safeClose(t, pom)
			safeClose(t, testClient)
		})
	}
}

// Test recovery from ErrNotCoordinatorForConsumer
// on first fetchInitialOffset call
func TestOffsetManagerFetchInitialFail(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, 0)

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
	newCoordinator := NewMockBroker(t, 3)
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
	safeClose(t, pom)
	safeClose(t, om)
	safeClose(t, testClient)
}

// Test fetchInitialOffset retry on ErrOffsetsLoadInProgress
func TestOffsetManagerFetchInitialLoadInProgress(t *testing.T) {
	retryCount := int32(0)
	backoff := func(retries, maxRetries int) time.Duration {
		atomic.AddInt32(&retryCount, 1)
		return 0
	}
	om, testClient, broker, coordinator := initOffsetManagerWithBackoffFunc(t, 0, backoff, NewConfig())

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
	safeClose(t, pom)
	safeClose(t, om)
	safeClose(t, testClient)

	if atomic.LoadInt32(&retryCount) == 0 {
		t.Fatal("Expected at least one retry")
	}
}

func TestPartitionOffsetManagerInitialOffset(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, 0)
	testClient.Config().Consumer.Offsets.Initial = OffsetOldest

	// Kafka returns -1 if no offset has been stored for this partition yet.
	pom := initPartitionOffsetManager(t, om, coordinator, -1, "")

	offset, meta := pom.NextOffset()
	if offset != OffsetOldest {
		t.Errorf("Expected offset 5. Actual: %v", offset)
	}
	if meta != "" {
		t.Errorf("Expected metadata to be empty. Actual: %q", meta)
	}

	safeClose(t, pom)
	safeClose(t, om)
	broker.Close()
	coordinator.Close()
	safeClose(t, testClient)
}

func TestPartitionOffsetManagerNextOffset(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, 0)
	pom := initPartitionOffsetManager(t, om, coordinator, 5, "test_meta")

	offset, meta := pom.NextOffset()
	if offset != 5 {
		t.Errorf("Expected offset 5. Actual: %v", offset)
	}
	if meta != "test_meta" {
		t.Errorf("Expected metadata \"test_meta\". Actual: %q", meta)
	}

	safeClose(t, pom)
	safeClose(t, om)
	broker.Close()
	coordinator.Close()
	safeClose(t, testClient)
}

func TestPartitionOffsetManagerResetOffset(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, 0)
	pom := initPartitionOffsetManager(t, om, coordinator, 5, "original_meta")

	ocResponse := new(OffsetCommitResponse)
	ocResponse.AddError("my_topic", 0, ErrNoError)
	coordinator.Returns(ocResponse)

	expected := int64(1)
	pom.ResetOffset(expected, "modified_meta")
	actual, meta := pom.NextOffset()

	if actual != expected {
		t.Errorf("Expected offset %v. Actual: %v", expected, actual)
	}
	if meta != "modified_meta" {
		t.Errorf("Expected metadata \"modified_meta\". Actual: %q", meta)
	}

	safeClose(t, pom)
	safeClose(t, om)
	safeClose(t, testClient)
	broker.Close()
	coordinator.Close()
}

func TestPartitionOffsetManagerResetOffsetWithRetention(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, time.Hour)
	pom := initPartitionOffsetManager(t, om, coordinator, 5, "original_meta")

	ocResponse := new(OffsetCommitResponse)
	ocResponse.AddError("my_topic", 0, ErrNoError)
	handler := func(req *request) (res encoder) {
		if req.body.version() != 2 {
			t.Errorf("Expected to be using version 2. Actual: %v", req.body.version())
		}
		offsetCommitRequest := req.body.(*OffsetCommitRequest)
		if offsetCommitRequest.RetentionTime != (60 * 60 * 1000) {
			t.Errorf("Expected an hour retention time. Actual: %v", offsetCommitRequest.RetentionTime)
		}
		return ocResponse
	}
	coordinator.setHandler(handler)

	expected := int64(1)
	pom.ResetOffset(expected, "modified_meta")
	actual, meta := pom.NextOffset()

	if actual != expected {
		t.Errorf("Expected offset %v. Actual: %v", expected, actual)
	}
	if meta != "modified_meta" {
		t.Errorf("Expected metadata \"modified_meta\". Actual: %q", meta)
	}

	safeClose(t, pom)
	safeClose(t, om)
	safeClose(t, testClient)
	broker.Close()
	coordinator.Close()
}

func TestPartitionOffsetManagerMarkOffset(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, 0)
	pom := initPartitionOffsetManager(t, om, coordinator, 5, "original_meta")

	ocResponse := new(OffsetCommitResponse)
	ocResponse.AddError("my_topic", 0, ErrNoError)
	coordinator.Returns(ocResponse)

	pom.MarkOffset(100, "modified_meta")
	offset, meta := pom.NextOffset()

	if offset != 100 {
		t.Errorf("Expected offset 100. Actual: %v", offset)
	}
	if meta != "modified_meta" {
		t.Errorf("Expected metadata \"modified_meta\". Actual: %q", meta)
	}

	safeClose(t, pom)
	safeClose(t, om)
	safeClose(t, testClient)
	broker.Close()
	coordinator.Close()
}

func TestPartitionOffsetManagerMarkOffsetWithRetention(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, time.Hour)
	pom := initPartitionOffsetManager(t, om, coordinator, 5, "original_meta")

	ocResponse := new(OffsetCommitResponse)
	ocResponse.AddError("my_topic", 0, ErrNoError)
	handler := func(req *request) (res encoder) {
		if req.body.version() != 2 {
			t.Errorf("Expected to be using version 2. Actual: %v", req.body.version())
		}
		offsetCommitRequest := req.body.(*OffsetCommitRequest)
		if offsetCommitRequest.RetentionTime != (60 * 60 * 1000) {
			t.Errorf("Expected an hour retention time. Actual: %v", offsetCommitRequest.RetentionTime)
		}
		return ocResponse
	}
	coordinator.setHandler(handler)

	pom.MarkOffset(100, "modified_meta")
	offset, meta := pom.NextOffset()

	if offset != 100 {
		t.Errorf("Expected offset 100. Actual: %v", offset)
	}
	if meta != "modified_meta" {
		t.Errorf("Expected metadata \"modified_meta\". Actual: %q", meta)
	}

	safeClose(t, pom)
	safeClose(t, om)
	safeClose(t, testClient)
	broker.Close()
	coordinator.Close()
}

func TestPartitionOffsetManagerCommitErr(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, 0)
	pom := initPartitionOffsetManager(t, om, coordinator, 5, "meta")

	// Error on one partition
	ocResponse := new(OffsetCommitResponse)
	ocResponse.AddError("my_topic", 0, ErrOffsetOutOfRange)
	ocResponse.AddError("my_topic", 1, ErrNoError)
	coordinator.Returns(ocResponse)

	newCoordinator := NewMockBroker(t, 3)

	// For RefreshCoordinator()
	broker.Returns(&ConsumerMetadataResponse{
		CoordinatorID:   newCoordinator.BrokerID(),
		CoordinatorHost: "127.0.0.1",
		CoordinatorPort: newCoordinator.Port(),
	})

	// Nothing in response.Errors at all
	ocResponse2 := new(OffsetCommitResponse)
	newCoordinator.Returns(ocResponse2)

	// No error, no need to refresh coordinator

	// Error on the wrong partition for this pom
	ocResponse3 := new(OffsetCommitResponse)
	ocResponse3.AddError("my_topic", 1, ErrNoError)
	newCoordinator.Returns(ocResponse3)

	// No error, no need to refresh coordinator

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

	pom.MarkOffset(100, "modified_meta")

	err := pom.Close()
	if err != nil {
		t.Error(err)
	}

	broker.Close()
	coordinator.Close()
	newCoordinator.Close()
	safeClose(t, om)
	safeClose(t, testClient)
}

// Test of recovery from abort
func TestAbortPartitionOffsetManager(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, 0)
	pom := initPartitionOffsetManager(t, om, coordinator, 5, "meta")

	// this triggers an error in the CommitOffset request,
	// which leads to the abort call
	coordinator.Close()

	// Response to refresh coordinator request
	newCoordinator := NewMockBroker(t, 3)
	broker.Returns(&ConsumerMetadataResponse{
		CoordinatorID:   newCoordinator.BrokerID(),
		CoordinatorHost: "127.0.0.1",
		CoordinatorPort: newCoordinator.Port(),
	})

	ocResponse := new(OffsetCommitResponse)
	ocResponse.AddError("my_topic", 0, ErrNoError)
	newCoordinator.Returns(ocResponse)

	pom.MarkOffset(100, "modified_meta")

	safeClose(t, pom)
	safeClose(t, om)
	broker.Close()
	safeClose(t, testClient)
}

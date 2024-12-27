//go:build !functional

package sarama

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func initOffsetManagerWithBackoffFunc(
	t *testing.T,
	retention time.Duration,
	backoffFunc func(retries, maxRetries int) time.Duration, config *Config,
) (om OffsetManager, testClient Client, broker, coordinator *MockBroker) {
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

	coordinator.Returns(&ConsumerMetadataResponse{
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
	testClient Client, broker, coordinator *MockBroker,
) {
	return initOffsetManagerWithBackoffFunc(t, retention, nil, NewTestConfig())
}

func initPartitionOffsetManager(t *testing.T, om OffsetManager,
	coordinator *MockBroker, initialOffset int64, metadata string,
) PartitionOffsetManager {
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
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	seedBroker.Returns(metadataResponse)
	findCoordResponse := new(FindCoordinatorResponse)
	findCoordResponse.Coordinator = &Broker{id: seedBroker.brokerID, addr: seedBroker.Addr()}
	seedBroker.Returns(findCoordResponse)
	defer seedBroker.Close()

	testClient, err := NewClient([]string{seedBroker.Addr()}, NewTestConfig())
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
	if !errors.Is(err, ErrClosedClient) {
		t.Errorf("Error expected for closed client; actual value: %v", err)
	}
}

// Test that the correct sequence of offset commit messages is sent to a broker when
// multiple goroutines for a group are committing offsets at the same time
func TestOffsetManagerCommitSequence(t *testing.T) {
	lastOffset := map[int32]int64{}
	var outOfOrder atomic.Pointer[string]
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()
	seedBroker.SetHandlerFuncByMap(map[string]requestHandlerFunc{
		"MetadataRequest": func(req *request) encoderWithHeader {
			resp := new(MetadataResponse)
			resp.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
			return resp
		},
		"FindCoordinatorRequest": func(req *request) encoderWithHeader {
			resp := new(FindCoordinatorResponse)
			resp.Coordinator = &Broker{id: seedBroker.brokerID, addr: seedBroker.Addr()}
			return resp
		},
		"OffsetFetchRequest": func(r *request) encoderWithHeader {
			req := r.body.(*OffsetFetchRequest)
			resp := new(OffsetFetchResponse)
			resp.Blocks = map[string]map[int32]*OffsetFetchResponseBlock{}
			for topic, partitions := range req.partitions {
				for _, partition := range partitions {
					if _, ok := resp.Blocks[topic]; !ok {
						resp.Blocks[topic] = map[int32]*OffsetFetchResponseBlock{}
					}
					resp.Blocks[topic][partition] = &OffsetFetchResponseBlock{
						Offset: 0,
						Err:    ErrNoError,
					}
				}
			}
			return resp
		},
		"OffsetCommitRequest": func(r *request) encoderWithHeader {
			req := r.body.(*OffsetCommitRequest)
			if outOfOrder.Load() == nil {
				for partition, offset := range req.blocks["topic"] {
					last := lastOffset[partition]
					if last > offset.offset {
						msg := fmt.Sprintf("out of order commit to partition %d, current committed offset: %d, offset in request: %d",
							partition, last, offset.offset)
						outOfOrder.Store(&msg)
					}
					lastOffset[partition] = offset.offset
				}
			}

			// Potentially yield, to try and avoid each Go routine running sequentially to completion
			runtime.Gosched()

			resp := new(OffsetCommitResponse)
			resp.Errors = map[string]map[int32]KError{}
			resp.Errors["topic"] = map[int32]KError{}
			for partition := range req.blocks["topic"] {
				resp.Errors["topic"][partition] = ErrNoError
			}
			return resp
		},
	})
	testClient, err := NewClient([]string{seedBroker.Addr()}, NewTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, testClient)
	om, err := NewOffsetManagerFromClient("group", testClient)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, om)

	const numPartitions = 10
	const commitsPerPartition = 1000

	var wg sync.WaitGroup
	for p := 0; p < numPartitions; p++ {
		pom, err := om.ManagePartition("topic", int32(p))
		if err != nil {
			t.Fatal(err)
		}

		wg.Add(1)
		go func() {
			for c := 0; c < commitsPerPartition; c++ {
				pom.MarkOffset(int64(c+1), "")
				om.Commit()
			}
			wg.Done()
		}()
	}

	wg.Wait()
	errMsg := outOfOrder.Load()
	if errMsg != nil {
		t.Error(*errMsg)
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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			config := NewTestConfig()
			if tt.set {
				config.Consumer.Offsets.AutoCommit.Enable = tt.enable
			}
			om, testClient, broker, coordinator := initOffsetManagerWithBackoffFunc(t, 0, nil, config)
			defer broker.Close()
			defer coordinator.Close()
			pom := initPartitionOffsetManager(t, om, coordinator, 5, "original_meta")

			// Wait long enough for the test not to fail..
			timeout := 50 * config.Consumer.Offsets.AutoCommit.Interval

			called := make(chan none)

			ocResponse := new(OffsetCommitResponse)
			ocResponse.AddError("my_topic", 0, ErrNoError)
			handler := func(req *request) (res encoderWithHeader) {
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

			// !! om must be closed before the pom so pom.release() is called before pom.Close()
			safeClose(t, om)
			safeClose(t, pom)
			safeClose(t, testClient)
		})
	}
}

func TestNewOffsetManagerOffsetsManualCommit(t *testing.T) {
	// Tests to validate configuration when `Consumer.Offsets.AutoCommit.Enable` is false
	config := NewTestConfig()
	config.Consumer.Offsets.AutoCommit.Enable = false

	om, testClient, broker, coordinator := initOffsetManagerWithBackoffFunc(t, 0, nil, config)
	defer broker.Close()
	defer coordinator.Close()
	pom := initPartitionOffsetManager(t, om, coordinator, 5, "original_meta")

	// Wait long enough for the test not to fail..
	timeout := 50 * config.Consumer.Offsets.AutoCommit.Interval

	ocResponse := new(OffsetCommitResponse)
	ocResponse.AddError("my_topic", 0, ErrNoError)
	called := make(chan none)
	handler := func(req *request) (res encoderWithHeader) {
		close(called)
		return ocResponse
	}
	coordinator.setHandler(handler)

	// Should not trigger an auto-commit
	expected := int64(1)
	pom.ResetOffset(expected, "modified_meta")
	_, _ = pom.NextOffset()

	select {
	case <-called:
		// OffsetManager called on the wire.
		t.Errorf("Received request when AutoCommit is disabled")
	case <-time.After(timeout):
		// Timeout waiting for OffsetManager to call on the wire.
		// OK
	}

	// Setup again to test manual commit
	called = make(chan none)

	om.Commit()

	select {
	case <-called:
		// OffsetManager called on the wire.
		// OK
	case <-time.After(timeout):
		// Timeout waiting for OffsetManager to call on the wire.
		t.Errorf("No request received for after waiting for %v", timeout)
	}

	// !! om must be closed before the pom so pom.release() is called before pom.Close()
	safeClose(t, om)
	safeClose(t, pom)
	safeClose(t, testClient)
}

// Test recovery from ErrNotCoordinatorForConsumer
// on first fetchInitialOffset call
func TestOffsetManagerFetchInitialFail(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, 0)
	defer broker.Close()
	defer coordinator.Close()

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
	defer newCoordinator.Close()
	coordinator.Returns(&ConsumerMetadataResponse{
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
	om, testClient, broker, coordinator := initOffsetManagerWithBackoffFunc(t, 0, backoff, NewTestConfig())
	defer broker.Close()
	defer coordinator.Close()

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

	safeClose(t, pom)
	safeClose(t, om)
	safeClose(t, testClient)

	if atomic.LoadInt32(&retryCount) == 0 {
		t.Fatal("Expected at least one retry")
	}
}

func TestPartitionOffsetManagerInitialOffset(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, 0)
	defer broker.Close()
	defer coordinator.Close()
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
	safeClose(t, testClient)
}

func TestPartitionOffsetManagerNextOffset(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, 0)
	defer broker.Close()
	defer coordinator.Close()
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
	safeClose(t, testClient)
}

func TestPartitionOffsetManagerResetOffset(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, 0)
	defer broker.Close()
	defer coordinator.Close()
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
}

func TestPartitionOffsetManagerResetOffsetWithRetention(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, time.Hour)
	defer broker.Close()
	defer coordinator.Close()
	pom := initPartitionOffsetManager(t, om, coordinator, 5, "original_meta")

	ocResponse := new(OffsetCommitResponse)
	ocResponse.AddError("my_topic", 0, ErrNoError)
	handler := func(req *request) (res encoderWithHeader) {
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
}

func TestPartitionOffsetManagerMarkOffset(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, 0)
	defer broker.Close()
	defer coordinator.Close()
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
}

func TestPartitionOffsetManagerMarkOffsetWithRetention(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, time.Hour)
	defer broker.Close()
	defer coordinator.Close()
	pom := initPartitionOffsetManager(t, om, coordinator, 5, "original_meta")

	ocResponse := new(OffsetCommitResponse)
	ocResponse.AddError("my_topic", 0, ErrNoError)
	handler := func(req *request) (res encoderWithHeader) {
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
}

func TestPartitionOffsetManagerCommitErr(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, 0)
	defer broker.Close()
	defer coordinator.Close()
	pom := initPartitionOffsetManager(t, om, coordinator, 5, "meta")

	// Error on one partition
	ocResponse := new(OffsetCommitResponse)
	ocResponse.AddError("my_topic", 0, ErrOffsetOutOfRange)
	ocResponse.AddError("my_topic", 1, ErrNoError)
	coordinator.Returns(ocResponse)

	// For RefreshCoordinator()
	coordinator.Returns(&ConsumerMetadataResponse{
		CoordinatorID:   coordinator.BrokerID(),
		CoordinatorHost: "127.0.0.1",
		CoordinatorPort: coordinator.Port(),
	})

	// Nothing in response.Errors at all
	ocResponse2 := new(OffsetCommitResponse)
	coordinator.Returns(ocResponse2)

	// No error, no need to refresh coordinator

	// Error on the wrong partition for this pom
	ocResponse3 := new(OffsetCommitResponse)
	ocResponse3.AddError("my_topic", 1, ErrNoError)
	coordinator.Returns(ocResponse3)

	// ErrUnknownTopicOrPartition/ErrNotLeaderForPartition/ErrLeaderNotAvailable block
	ocResponse4 := new(OffsetCommitResponse)
	ocResponse4.AddError("my_topic", 0, ErrUnknownTopicOrPartition)
	coordinator.Returns(ocResponse4)

	newCoordinator := NewMockBroker(t, 3)
	defer newCoordinator.Close()

	// For RefreshCoordinator()
	coordinator.Returns(&ConsumerMetadataResponse{
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

	safeClose(t, om)
	safeClose(t, testClient)
}

// Test of recovery from abort
func TestAbortPartitionOffsetManager(t *testing.T) {
	om, testClient, broker, coordinator := initOffsetManager(t, 0)
	defer broker.Close()
	pom := initPartitionOffsetManager(t, om, coordinator, 5, "meta")

	// this triggers an error in the CommitOffset request,
	// which leads to the abort call
	coordinator.Close()

	// Response to refresh coordinator request
	newCoordinator := NewMockBroker(t, 3)
	defer newCoordinator.Close()
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
	safeClose(t, testClient)
}

// Validate that the constructRequest() method correctly maps Sarama's default for
// Config.Consumer.Offsets.Retention to the equivalent Kafka value.
func TestConstructRequestRetentionTime(t *testing.T) {
	expectedRetention := func(version KafkaVersion, retention time.Duration) int64 {
		switch {
		case version.IsAtLeast(V2_1_0_0):
			// version >= 2.1.0: Client specified retention time isn't supported in the
			// offset commit request anymore, thus the retention time field set in the
			// OffsetCommitRequest struct should be 0.
			return 0
		case version.IsAtLeast(V0_9_0_0):
			// 0.9.0 <= version < 2.1.0: Retention time *is* supported in the offset commit
			// request. Sarama's default retention times (0) must be mapped to the Kafka
			// default (-1). Non-zero Sarama times are converted from time.Duration to
			// an int64 millisecond value.
			if retention > 0 {
				return int64(retention / time.Millisecond)
			} else {
				return -1
			}
		default:
			// version < 0.9.0: Client specified retention time is not supported in the offset
			// commit request, thus the retention time field set in the OffsetCommitRequest
			// struct should be 0.
			return 0
		}
	}

	for _, version := range SupportedVersions {
		for _, retention := range []time.Duration{0, time.Millisecond} {
			name := fmt.Sprintf("version %s retention: %s", version, retention)
			t.Run(name, func(t *testing.T) {
				// Perform necessary setup for calling the constructRequest() method. This
				// test-case only cares about the code path that sets the retention time
				// field in the returned request struct.
				conf := NewTestConfig()
				conf.Version = version
				conf.Consumer.Offsets.Retention = retention
				om := &offsetManager{
					conf: conf,
					poms: map[string]map[int32]*partitionOffsetManager{
						"topic": {
							0: {
								dirty: true,
							},
						},
					},
				}

				req := om.constructRequest()

				expectedRetention := expectedRetention(version, retention)
				if req.RetentionTime != expectedRetention {
					t.Errorf("expected retention time %d, got: %d", expectedRetention, req.RetentionTime)
				}
			})
		}
	}
}

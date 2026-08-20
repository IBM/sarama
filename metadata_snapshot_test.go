//go:build !functional

package sarama

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetadataSnapshotReturnsDetachedCachedStateWithoutRefresh(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	t.Cleanup(seedBroker.Close)

	rack := "rack-a"
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadataResponse.Brokers[0].rack = &rack
	metadataResponse.AddBroker("broker-2:9092", 2)
	metadataResponse.ControllerID = 2
	metadataResponse.AddTopicPartition(
		"topic",
		0,
		seedBroker.BrokerID(),
		[]int32{1, 2},
		[]int32{1},
		[]int32{2},
		ErrNoError,
	)
	metadataResponse.Topics[0].Partitions[0].LeaderEpoch = 9
	seedBroker.Returns(metadataResponse)

	config := NewConfig()
	config.ApiVersionsRequest = false
	config.Version = V2_8_0_0
	config.Metadata.Retry.Max = 0
	baseClient, err := NewClient([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		if !baseClient.Closed() {
			require.NoError(t, baseClient.Close())
		}
	})

	client, ok := baseClient.(MetadataSnapshotterClient)
	require.True(t, ok)

	expectedRack := "rack-a"
	expected := &MetadataSnapshot{
		ControllerID: 2,
		Brokers: map[int32]BrokerSnapshot{
			1: {Addr: seedBroker.Addr(), Rack: &expectedRack},
			2: {Addr: "broker-2:9092"},
		},
		Topics: map[string]map[int32]PartitionSnapshot{
			"topic": {
				0: {
					Version:         11,
					Err:             ErrNoError,
					ID:              0,
					Leader:          1,
					LeaderEpoch:     9,
					Replicas:        []int32{1, 2},
					Isr:             []int32{1},
					OfflineReplicas: []int32{2},
				},
			},
		},
	}

	requestsBeforeSnapshot := len(seedBroker.History())
	snapshot, err := client.MetadataSnapshot()
	require.NoError(t, err)
	require.Len(
		t,
		seedBroker.History(),
		requestsBeforeSnapshot,
		"MetadataSnapshot must not issue another Kafka request",
	)
	require.Equal(t, expected, snapshot)

	snapshot.ControllerID = 99
	broker := snapshot.Brokers[1]
	broker.Addr = "changed:9092"
	*broker.Rack = "rack-b"
	snapshot.Brokers[1] = broker
	delete(snapshot.Brokers, 2)
	partition := snapshot.Topics["topic"][0]
	partition.Leader = 99
	partition.Replicas[0] = 99
	partition.Isr[0] = 99
	partition.OfflineReplicas[0] = 99
	snapshot.Topics["topic"][0] = partition
	snapshot.Topics["added"] = map[int32]PartitionSnapshot{}

	snapshot, err = client.MetadataSnapshot()
	require.NoError(t, err)
	require.Equal(t, expected, snapshot, "mutating a snapshot must not affect the client cache")

	require.NoError(t, baseClient.Close())
	snapshot, err = client.MetadataSnapshot()
	require.ErrorIs(t, err, ErrClosedClient, "MetadataSnapshot must report a closed client after Close")
	require.Nil(t, snapshot)
}

func TestMetadataSnapshotReturnsPartialCacheAfterIncompleteRefresh(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	t.Cleanup(seedBroker.Close)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadataResponse.AddTopicPartition(
		"topic",
		0,
		seedBroker.BrokerID(),
		[]int32{seedBroker.BrokerID()},
		[]int32{seedBroker.BrokerID()},
		[]int32{},
		ErrNoError,
	)
	metadataResponse.AddTopicPartition(
		"topic",
		1,
		-1,
		[]int32{seedBroker.BrokerID()},
		[]int32{},
		[]int32{},
		ErrLeaderNotAvailable,
	)
	seedBroker.Returns(metadataResponse)

	config := NewConfig()
	config.ApiVersionsRequest = false
	config.Metadata.Retry.Max = 0
	baseClient, err := NewClient([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, baseClient.Close())
	})

	client, ok := baseClient.(MetadataSnapshotterClient)
	require.True(t, ok)

	snapshot, err := client.MetadataSnapshot()

	require.NoError(t, err)
	require.Len(t, snapshot.Topics["topic"], 2,
		"MetadataSnapshot must expose all metadata cached by an incomplete refresh")
	require.Equal(t, ErrNoError, snapshot.Topics["topic"][0].Err)
	require.Equal(t, ErrLeaderNotAvailable, snapshot.Topics["topic"][1].Err)
}

func TestMetadataSnapshotWithMetadataFullDisabled(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	t.Cleanup(seedBroker.Close)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadataResponse.ControllerID = seedBroker.BrokerID()
	metadataResponse.AddTopicPartition(
		"topic-a",
		0,
		seedBroker.BrokerID(),
		[]int32{seedBroker.BrokerID()},
		[]int32{seedBroker.BrokerID()},
		[]int32{},
		ErrNoError,
	)
	seedBroker.Returns(metadataResponse)

	config := NewConfig()
	config.ApiVersionsRequest = false
	config.Version = V2_8_0_0
	config.Metadata.Full = false
	config.Metadata.RefreshFrequency = 0
	config.Metadata.Retry.Max = 0
	baseClient, err := NewClient([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, baseClient.Close())
	})

	client, ok := baseClient.(MetadataSnapshotterClient)
	require.True(t, ok)
	require.Empty(t, seedBroker.History(), "NewClient must not fetch metadata when Metadata.Full is false")

	snapshot, err := client.MetadataSnapshot()

	require.NoError(t, err)
	require.Equal(t, int32(-1), snapshot.ControllerID)
	require.Empty(t, snapshot.Brokers)
	require.Empty(t, snapshot.Topics)
	require.Empty(t, seedBroker.History(), "MetadataSnapshot must not fetch metadata")

	require.NoError(t, baseClient.RefreshMetadata("topic-a"))
	requestsBeforeSnapshot := len(seedBroker.History())
	snapshot, err = client.MetadataSnapshot()
	require.NoError(t, err)
	require.Equal(t, seedBroker.BrokerID(), snapshot.ControllerID)
	require.Contains(t, snapshot.Topics, "topic-a")
	require.Len(t, seedBroker.History(), requestsBeforeSnapshot,
		"MetadataSnapshot must not issue another Kafka request")
}

func TestMetadataSnapshotReportsUnknownControllerForMetadataVersionZero(t *testing.T) {
	seedBroker := NewMockBroker(t, 0)
	t.Cleanup(seedBroker.Close)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadataResponse.ControllerID = 7
	seedBroker.Returns(metadataResponse)

	config := NewConfig()
	config.ApiVersionsRequest = false
	config.Version = V0_8_2_0
	config.Metadata.Retry.Max = 0
	baseClient, err := NewClient([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, baseClient.Close())
	})

	client, ok := baseClient.(MetadataSnapshotterClient)
	require.True(t, ok)

	snapshot, err := client.MetadataSnapshot()
	require.NoError(t, err)
	require.Equal(t, int32(-1), snapshot.ControllerID)
}

func TestMetadataSnapshotConsistentDuringConcurrentUpdates(t *testing.T) {
	type metadataState struct {
		response *MetadataResponse
		snapshot *MetadataSnapshot
	}

	newState := func(controllerID, partitionID int32) metadataState {
		response := &MetadataResponse{Version: 11, ControllerID: controllerID}
		response.AddBroker("broker-a:9092", 1)
		response.AddBroker("broker-b:9092", 2)
		response.AddTopicPartition(
			"topic",
			partitionID,
			controllerID,
			[]int32{controllerID},
			[]int32{controllerID},
			[]int32{},
			ErrNoError,
		)
		response.Topics[0].Partitions[0].Version = response.Version

		return metadataState{
			response: response,
			snapshot: &MetadataSnapshot{
				ControllerID: controllerID,
				Brokers: map[int32]BrokerSnapshot{
					1: {Addr: "broker-a:9092"},
					2: {Addr: "broker-b:9092"},
				},
				Topics: map[string]map[int32]PartitionSnapshot{
					"topic": {
						partitionID: {
							Version:         11,
							ID:              partitionID,
							Leader:          controllerID,
							Replicas:        []int32{controllerID},
							Isr:             []int32{controllerID},
							OfflineReplicas: []int32{},
						},
					},
				},
			},
		}
	}
	states := []metadataState{
		newState(1, 0),
		newState(2, 1),
	}

	client := &client{
		conf:                    NewConfig(),
		brokers:                 make(map[int32]*Broker),
		metadata:                make(map[string]map[int32]*PartitionMetadata),
		metadataTopics:          make(map[string]none),
		cachedPartitionsResults: make(map[string][maxPartitionIndex][]int32),
	}
	started := make(chan struct{})
	done := make(chan struct{})
	writerErr := make(chan error, 1)
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for iteration := 0; ; iteration++ {
			state := states[iteration%len(states)]
			if _, err := client.updateMetadata(state.response, false); err != nil {
				writerErr <- err
				return
			}

			if iteration == 0 {
				close(started)
			}
			select {
			case <-done:
				return
			default:
			}
		}
	}()
	<-started
	defer func() {
		close(done)
		writer.Wait()
	}()

	for range 10_000 {
		select {
		case err := <-writerErr:
			t.Fatalf("update metadata: %v", err)
		default:
		}
		snapshot, err := client.MetadataSnapshot()
		require.NoError(t, err)
		if !reflect.DeepEqual(snapshot, states[0].snapshot) &&
			!reflect.DeepEqual(snapshot, states[1].snapshot) {
			t.Fatalf("metadata snapshot combines multiple cache states: %#v", snapshot)
		}
	}
}

func BenchmarkMetadataSnapshot(b *testing.B) {
	for _, partitionCount := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("partitions-%d", partitionCount), func(b *testing.B) {
			partitions := make(map[int32]*PartitionMetadata, partitionCount)
			for partitionID := range partitionCount {
				id := int32(partitionID)
				partitions[id] = &PartitionMetadata{
					ID:              id,
					Leader:          1,
					Replicas:        []int32{1, 2, 3},
					Isr:             []int32{1, 2},
					OfflineReplicas: []int32{3},
				}
			}
			client := &client{
				brokers:  map[int32]*Broker{1: NewBroker("broker:9092")},
				metadata: map[string]map[int32]*PartitionMetadata{"topic": partitions},
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := client.MetadataSnapshot(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

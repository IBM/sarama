package sarama

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetadataSnapshotCopyWithoutRefresh(t *testing.T) {
	refreshes := 0
	rack := "rack-a"
	brokerWithRack := NewBroker("broker-1:9092")
	brokerWithRack.rack = &rack
	client := &client{
		controllerID: 2,
		brokers: map[int32]*Broker{
			1: brokerWithRack,
			2: NewBroker("broker-2:9092"),
		},
		metadata: map[string]map[int32]*PartitionMetadata{
			"topic": {
				0: {
					Version:         7,
					Err:             ErrReplicaNotAvailable,
					ID:              0,
					Leader:          1,
					LeaderEpoch:     9,
					Replicas:        []int32{1, 2},
					Isr:             []int32{1},
					OfflineReplicas: []int32{2},
				},
			},
		},
		metadataRefresh: func([]string) error {
			refreshes++
			return nil
		},
	}
	client.updateMetadataMs.Store(time.Now().UnixMilli())

	snapshot, err := client.MetadataSnapshot()

	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.Zero(t, refreshes)
	require.Equal(t, int32(2), snapshot.ControllerID)
	require.Equal(t, map[int32]BrokerSnapshot{
		1: {Addr: "broker-1:9092", Rack: &rack},
		2: {Addr: "broker-2:9092"},
	}, snapshot.Brokers)
	require.Equal(t, PartitionSnapshot{
		Version:         7,
		Err:             ErrReplicaNotAvailable,
		ID:              0,
		Leader:          1,
		LeaderEpoch:     9,
		Replicas:        []int32{1, 2},
		Isr:             []int32{1},
		OfflineReplicas: []int32{2},
	}, snapshot.Topics["topic"][0])

	broker := snapshot.Brokers[1]
	broker.Addr = "changed:9092"
	*broker.Rack = "rack-b"
	snapshot.Brokers[1] = broker
	partition := snapshot.Topics["topic"][0]
	partition.Replicas[0] = 99
	partition.Isr[0] = 99
	partition.OfflineReplicas[0] = 99
	snapshot.Topics["topic"][0] = PartitionSnapshot{Leader: 99}

	require.Equal(t, "broker-1:9092", client.brokers[1].Addr())
	require.Equal(t, "rack-a", client.brokers[1].Rack())
	require.Equal(t, int32(1), client.metadata["topic"][0].Leader)
	require.Equal(t, []int32{1, 2}, client.metadata["topic"][0].Replicas)
	require.Equal(t, []int32{1}, client.metadata["topic"][0].Isr)
	require.Equal(t, []int32{2}, client.metadata["topic"][0].OfflineReplicas)
}

func TestMetadataSnapshotAvailability(t *testing.T) {
	tests := []struct {
		name             string
		brokers          map[int32]*Broker
		metadataUpdated  bool
		expectedSnapshot *MetadataSnapshot
		expectedError    error
	}{
		{
			name:          "before metadata refresh",
			brokers:       map[int32]*Broker{},
			expectedError: ErrMetadataNotInitialized,
		},
		{
			name:            "after empty metadata refresh",
			brokers:         map[int32]*Broker{},
			metadataUpdated: true,
			expectedSnapshot: &MetadataSnapshot{
				Brokers: map[int32]BrokerSnapshot{},
				Topics:  map[string]map[int32]PartitionSnapshot{},
			},
		},
		{
			name:            "after client close",
			metadataUpdated: true,
			expectedError:   ErrClosedClient,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &client{
				brokers:  test.brokers,
				metadata: map[string]map[int32]*PartitionMetadata{},
			}
			if test.metadataUpdated {
				client.updateMetadataMs.Store(time.Now().UnixMilli())
			}

			snapshot, err := client.MetadataSnapshot()
			if test.expectedError != nil {
				require.ErrorIs(t, err, test.expectedError)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.expectedSnapshot, snapshot)
		})
	}
}

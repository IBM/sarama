package sarama

import "slices"

// MetadataSnapshotterClient extends Client with access to its cached metadata.
// Clients returned by NewClient implement MetadataSnapshotterClient.
type MetadataSnapshotterClient interface {
	Client

	// MetadataSnapshot returns a detached copy of the current cached metadata
	// without sending a request or triggering a metadata refresh.
	//
	// It returns ErrClosedClient if the client is closed. Before metadata has
	// been cached, it returns an empty snapshot.
	MetadataSnapshot() (*MetadataSnapshot, error)
}

// MetadataSnapshot contains a point-in-time copy of a client's cached cluster
// metadata. Modifying a snapshot does not affect the client cache.
type MetadataSnapshot struct {
	// ControllerID is the broker ID of the cluster controller, or -1 if the
	// controller is not yet known or the metadata protocol does not report it.
	ControllerID int32
	// Brokers maps broker IDs to their cached metadata.
	Brokers map[int32]BrokerSnapshot
	// Topics maps topic names and partition IDs to their cached metadata. It may
	// not contain all cluster topics when Config.Metadata.Full is false.
	Topics map[string]map[int32]PartitionSnapshot
}

// BrokerSnapshot contains the cluster metadata cached for a broker.
type BrokerSnapshot struct {
	// Addr is the broker address advertised in cluster metadata.
	Addr string
	// Rack is the broker rack advertised in cluster metadata, or nil if the
	// broker did not advertise a rack.
	Rack *string
}

// PartitionSnapshot contains the cluster metadata cached for a partition.
type PartitionSnapshot struct {
	// Version is the metadata protocol version used to decode the partition.
	Version int16
	// Err is the partition error, or ErrNoError if there was no error.
	Err KError
	// ID is the partition ID.
	ID int32
	// Leader is the broker ID of the partition leader.
	Leader int32
	// LeaderEpoch is the epoch of the partition leader.
	LeaderEpoch int32
	// Replicas contains the broker IDs of all partition replicas.
	Replicas []int32
	// Isr contains the broker IDs of all in-sync replicas.
	Isr []int32
	// OfflineReplicas contains the broker IDs of all offline replicas.
	OfflineReplicas []int32
}

// MetadataSnapshot returns a detached copy of the current cached metadata
// without sending a request or triggering a metadata refresh.
func (client *client) MetadataSnapshot() (*MetadataSnapshot, error) {
	client.lock.RLock()
	defer client.lock.RUnlock()

	if client.brokers == nil {
		return nil, ErrClosedClient
	}

	snapshot := &MetadataSnapshot{
		ControllerID: client.controllerID,
		Brokers:      make(map[int32]BrokerSnapshot, len(client.brokers)),
		Topics:       make(map[string]map[int32]PartitionSnapshot, len(client.metadata)),
	}

	for id, broker := range client.brokers {
		brokerSnapshot := BrokerSnapshot{Addr: broker.Addr()}
		if broker.rack != nil {
			// Copy the pointed-to string so the snapshot cannot mutate the client cache.
			rack := *broker.rack
			brokerSnapshot.Rack = &rack
		}
		snapshot.Brokers[id] = brokerSnapshot
	}

	for topic, partitions := range client.metadata {
		snapshot.Topics[topic] = make(map[int32]PartitionSnapshot, len(partitions))
		for id, metadata := range partitions {
			snapshot.Topics[topic][id] = PartitionSnapshot{
				Version:         metadata.Version,
				Err:             metadata.Err,
				ID:              metadata.ID,
				Leader:          metadata.Leader,
				LeaderEpoch:     metadata.LeaderEpoch,
				Replicas:        slices.Clone(metadata.Replicas),
				Isr:             slices.Clone(metadata.Isr),
				OfflineReplicas: slices.Clone(metadata.OfflineReplicas),
			}
		}
	}

	return snapshot, nil
}

var _ MetadataSnapshotterClient = (*client)(nil)

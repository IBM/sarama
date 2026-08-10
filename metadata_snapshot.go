package sarama

import "slices"

// MetadataSnapshotterClient extends Client with access to its cached metadata.
// Clients returned by NewClient implement MetadataSnapshotterClient.
type MetadataSnapshotterClient interface {
	Client

	// MetadataSnapshot returns a detached copy of the current cached metadata
	// without sending a request or triggering a metadata refresh.
	//
	// It returns ErrClosedClient if the client is closed, or
	// ErrMetadataNotInitialized if the client has not completed a metadata refresh.
	MetadataSnapshot() (*MetadataSnapshot, error)
}

// MetadataSnapshot contains a point-in-time copy of a client's cached cluster
// metadata. Modifying a snapshot does not affect the client cache.
type MetadataSnapshot struct {
	// ControllerID is the broker ID of the cluster controller.
	ControllerID int32
	// Brokers maps broker IDs to their cached metadata.
	Brokers map[int32]BrokerMetadata
	// Topics maps topic names and partition IDs to their cached metadata.
	Topics map[string]map[int32]PartitionMetadata
}

// BrokerMetadata contains the cluster metadata cached for a broker.
type BrokerMetadata struct {
	// Addr is the broker address advertised in cluster metadata.
	Addr string
	// Rack is the broker rack advertised in cluster metadata, or nil if the
	// broker did not advertise a rack.
	Rack *string
}

// MetadataSnapshot returns a detached copy of the current cached metadata
// without sending a request or triggering a metadata refresh.
func (client *client) MetadataSnapshot() (*MetadataSnapshot, error) {
	client.lock.RLock()
	defer client.lock.RUnlock()

	if client.brokers == nil {
		return nil, ErrClosedClient
	}
	if client.updateMetadataMs.Load() == 0 {
		return nil, ErrMetadataNotInitialized
	}

	snapshot := &MetadataSnapshot{
		ControllerID: client.controllerID,
		Brokers:      make(map[int32]BrokerMetadata, len(client.brokers)),
		Topics:       make(map[string]map[int32]PartitionMetadata, len(client.metadata)),
	}

	for id, broker := range client.brokers {
		metadata := BrokerMetadata{Addr: broker.Addr()}
		if broker.rack != nil {
			rack := *broker.rack
			metadata.Rack = &rack
		}
		snapshot.Brokers[id] = metadata
	}

	for topic, partitions := range client.metadata {
		snapshot.Topics[topic] = make(map[int32]PartitionMetadata, len(partitions))
		for id, metadata := range partitions {
			partition := *metadata
			partition.Replicas = slices.Clone(metadata.Replicas)
			partition.Isr = slices.Clone(metadata.Isr)
			partition.OfflineReplicas = slices.Clone(metadata.OfflineReplicas)
			snapshot.Topics[topic][id] = partition
		}
	}

	return snapshot, nil
}

var _ MetadataSnapshotterClient = (*client)(nil)

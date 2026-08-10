package sarama

import "slices"

// MetadataSnapshotterClient extends Client with access to its cached metadata.
// Clients returned by NewClient implement MetadataSnapshotterClient.
type MetadataSnapshotterClient interface {
	Client

	// MetadataSnapshot returns a detached copy of the current cached metadata
	// without sending a request or triggering a metadata refresh. It returns nil
	// if the client is closed or has not completed a metadata refresh.
	MetadataSnapshot() *MetadataSnapshot
}

// MetadataSnapshot contains a point-in-time copy of a client's cached cluster
// metadata. Modifying a snapshot does not affect the client cache.
type MetadataSnapshot struct {
	Brokers map[int32]string
	Topics  map[string]map[int32]PartitionMetadata
}

// MetadataSnapshot returns a detached copy of the current cached metadata
// without sending a request or triggering a metadata refresh.
func (client *client) MetadataSnapshot() *MetadataSnapshot {
	client.lock.RLock()
	defer client.lock.RUnlock()

	if client.brokers == nil || client.updateMetadataMs.Load() == 0 {
		return nil
	}

	snapshot := &MetadataSnapshot{
		Brokers: make(map[int32]string, len(client.brokers)),
		Topics:  make(map[string]map[int32]PartitionMetadata, len(client.metadata)),
	}

	for id, broker := range client.brokers {
		snapshot.Brokers[id] = broker.Addr()
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

	return snapshot
}

var _ MetadataSnapshotterClient = (*client)(nil)

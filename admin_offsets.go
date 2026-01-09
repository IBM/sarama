package sarama

import (
	"sync"
	"time"
)

// TopicPartitionID identifies a specific partition in a topic.
type TopicPartitionID struct {
	Topic     string
	Partition int32
}

// OffsetSpec specifies which offset to look up for a partition.
type OffsetSpec struct {
	timestamp int64
}

// OffsetSpecLatest requests the log end offset.
func OffsetSpecLatest() OffsetSpec {
	return OffsetSpec{timestamp: OffsetNewest}
}

// OffsetSpecEarliest requests the log start offset.
func OffsetSpecEarliest() OffsetSpec {
	return OffsetSpec{timestamp: OffsetOldest}
}

// OffsetSpecForTimestamp requests the offset for the given timestamp in milliseconds.
func OffsetSpecForTimestamp(timestamp int64) OffsetSpec {
	return OffsetSpec{timestamp: timestamp}
}

// Timestamp returns the underlying timestamp value for this spec.
func (s OffsetSpec) Timestamp() int64 {
	return s.timestamp
}

// ListOffsetsOptions configures how offsets are fetched.
type ListOffsetsOptions struct {
	IsolationLevel IsolationLevel
}

// ListOffsetsResult contains the response for a single topic partition.
type ListOffsetsResult struct {
	Offset      int64
	Timestamp   int64
	LeaderEpoch int32
	Err         error
}

// OffsetAndMetadata describes the offset commit for a single partition.
type OffsetAndMetadata struct {
	Offset   int64
	Metadata string
	// LeaderEpoch contains the leader epoch of the last consumed record.
	// It is used by the broker to fence stale commits after leader changes.
	// Use the epoch returned by offset fetch/list APIs, or -1 to omit.
	LeaderEpoch int32
}

// AlterConsumerGroupOffsetsOptions configures how offsets are committed.
// It is currently empty and reserved for future Kafka protocol options.
type AlterConsumerGroupOffsetsOptions struct{}

type brokerOffsetRequest struct {
	broker     *Broker
	request    *OffsetRequest
	partitions []TopicPartitionID
}

type brokerOffsetResult struct {
	result map[TopicPartitionID]*ListOffsetsResult
	err    error
}

func (ca *clusterAdmin) ListOffsets(partitions map[TopicPartitionID]OffsetSpec, options *ListOffsetsOptions) (map[TopicPartitionID]*ListOffsetsResult, error) {
	if len(partitions) == 0 {
		return nil, ConfigurationError("no partitions provided")
	}

	if options == nil {
		options = &ListOffsetsOptions{}
	}

	requests := make(map[*Broker]*brokerOffsetRequest)
	for tp, spec := range partitions {
		broker, _, err := ca.client.LeaderAndEpoch(tp.Topic, tp.Partition)
		if err != nil {
			return nil, err
		}

		req := requests[broker]
		if req == nil {
			req = &brokerOffsetRequest{
				broker:  broker,
				request: NewOffsetRequest(ca.conf.Version),
			}
			req.request.IsolationLevel = options.IsolationLevel
			requests[broker] = req
		}
		req.request.AddBlock(tp.Topic, tp.Partition, spec.timestamp, 1)
		req.partitions = append(req.partitions, tp)
	}

	results := make(chan brokerOffsetResult, len(requests))
	var wg sync.WaitGroup

	for _, req := range requests {
		wg.Add(1)
		go func(req *brokerOffsetRequest) {
			defer wg.Done()

			resp, err := req.broker.GetAvailableOffsets(req.request)
			if err != nil {
				results <- brokerOffsetResult{err: err}
				return
			}
			req.broker.handleThrottledResponse(resp)

			partitionResults := make(map[TopicPartitionID]*ListOffsetsResult, len(req.partitions))
			for _, tp := range req.partitions {
				block := resp.GetBlock(tp.Topic, tp.Partition)
				if block == nil {
					partitionResults[tp] = &ListOffsetsResult{Err: ErrIncompleteResponse}
					continue
				}
				partitionResults[tp] = &ListOffsetsResult{
					Offset:      block.Offset,
					Timestamp:   block.Timestamp,
					LeaderEpoch: block.LeaderEpoch,
					Err:         block.Err,
				}
			}

			results <- brokerOffsetResult{result: partitionResults}
		}(req)
	}

	wg.Wait()
	close(results)

	allResults := make(map[TopicPartitionID]*ListOffsetsResult, len(partitions))
	var firstErr error
	for res := range results {
		if res.err != nil && firstErr == nil {
			firstErr = res.err
		}
		for tp, info := range res.result {
			allResults[tp] = info
		}
	}

	return allResults, firstErr
}

func (ca *clusterAdmin) AlterConsumerGroupOffsets(group string, offsets map[TopicPartitionID]OffsetAndMetadata, _ *AlterConsumerGroupOffsetsOptions) (*OffsetCommitResponse, error) {
	if len(offsets) == 0 {
		return nil, ConfigurationError("no offsets provided")
	}

	var response *OffsetCommitResponse
	request := newOffsetCommitRequestForAdmin(ca.conf, group)

	var commitTimestamp int64
	if request.Version == 1 {
		commitTimestamp = ReceiveTime
	}

	for tp, offset := range offsets {
		request.AddBlockWithLeaderEpoch(tp.Topic, tp.Partition, offset.Offset, offset.LeaderEpoch, commitTimestamp, offset.Metadata)
	}

	err := ca.retryOnError(isRetriableGroupCoordinatorError, func() (err error) {
		defer func() {
			if err != nil && isRetriableGroupCoordinatorError(err) {
				_ = ca.client.RefreshCoordinator(group)
			}
		}()

		coordinator, err := ca.client.Coordinator(group)
		if err != nil {
			return err
		}

		response, err = coordinator.CommitOffset(request)
		if err != nil {
			return err
		}

		return nil
	})

	return response, err
}

func newOffsetCommitRequestForAdmin(conf *Config, group string) *OffsetCommitRequest {
	request := &OffsetCommitRequest{
		ConsumerGroup:           group,
		ConsumerGroupGeneration: GroupGenerationUndefined,
	}

	if conf.Version.IsAtLeast(V0_9_0_0) {
		request.Version = 2
	} else {
		request.Version = 1
	}
	if conf.Version.IsAtLeast(V0_11_0_0) {
		request.Version = 3
	}
	if conf.Version.IsAtLeast(V2_0_0_0) {
		request.Version = 4
	}
	if conf.Version.IsAtLeast(V2_1_0_0) {
		request.Version = 6
	}
	if conf.Version.IsAtLeast(V2_3_0_0) {
		request.Version = 7
	}

	if request.Version >= 2 && request.Version < 5 {
		request.RetentionTime = -1
		if conf.Consumer.Offsets.Retention > 0 {
			request.RetentionTime = conf.Consumer.Offsets.Retention.Milliseconds()
		}
	}

	return request
}

package sarama

import (
	"errors"
	"io"
	"sync"
)

// TransactionClusterAdmin extends ClusterAdmin with the read-only KIP-664
// transaction-detection APIs (DescribeProducers, DescribeTransactions and
// ListTransactions). The value returned by NewClusterAdmin and
type TransactionClusterAdmin interface {
	ClusterAdmin

	// DescribeProducers lists the active producers for the given topic
	// partitions, querying each partition's leader. Requires Kafka 2.8.0.0 or
	// higher.
	DescribeProducers(topicPartitions map[string][]int32) (map[string]map[int32]DescribeProducersResponsePartition, error)

	// DescribeTransactions returns the current state of the given transactional
	// ids, querying each transaction's coordinator. Requires Kafka 3.0.0.0 or
	// higher.
	DescribeTransactions(transactionalIDs []string) (map[string]TransactionState, error)

	// ListTransactions lists the transactions known to the cluster, optionally
	// filtered by state, producer id, or (Kafka 3.8.0.0+) minimum duration in
	// milliseconds. Requires Kafka 3.0.0.0 or higher. A durationFilterMs less
	// than 0 disables the duration filter.
	ListTransactions(stateFilters []string, producerIDFilters []int64, durationFilterMs int64) ([]ListTransactionsResponseTransactionState, error)
}

var _ TransactionClusterAdmin = (*clusterAdmin)(nil)

func (ca *clusterAdmin) DescribeProducers(topicPartitions map[string][]int32) (map[string]map[int32]DescribeProducersResponsePartition, error) {
	result := make(map[string]map[int32]DescribeProducersResponsePartition)
	if len(topicPartitions) == 0 {
		return result, nil
	}

	type topicPartition struct {
		topic     string
		partition int32
	}

	// Group the requested partitions by their leader broker so a single request
	// is sent to each leader.
	partitionsPerBroker := make(map[*Broker][]topicPartition)
	var errs []error
	for topic, partitions := range topicPartitions {
		for _, partition := range partitions {
			leader, err := ca.client.Leader(topic, partition)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			partitionsPerBroker[leader] = append(partitionsPerBroker[leader], topicPartition{topic, partition})
		}
	}

	for broker, topicPartitions := range partitionsPerBroker {
		partitionsPerTopic := make(map[string][]int32)
		for _, tp := range topicPartitions {
			partitionsPerTopic[tp.topic] = append(partitionsPerTopic[tp.topic], tp.partition)
		}

		request := &DescribeProducersRequest{}
		for topic, partitions := range partitionsPerTopic {
			request.Topics = append(request.Topics, DescribeProducersRequestTopic{
				Name:             topic,
				PartitionIndexes: partitions,
			})
		}

		response, err := broker.DescribeProducers(request)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		for _, topic := range response.Topics {
			for _, partition := range topic.Partitions {
				if result[topic.Name] == nil {
					result[topic.Name] = make(map[int32]DescribeProducersResponsePartition)
				}
				result[topic.Name][partition.PartitionIndex] = partition
			}
		}
	}

	return result, errors.Join(errs...)
}

func (ca *clusterAdmin) DescribeTransactions(transactionalIDs []string) (map[string]TransactionState, error) {
	result := make(map[string]TransactionState)
	if len(transactionalIDs) == 0 {
		return result, nil
	}

	// Group the transactional ids by their coordinating broker so a single
	// request is sent to each coordinator.
	idsPerCoordinator := make(map[*Broker][]string)
	var errs []error
	for _, id := range transactionalIDs {
		coordinator, err := ca.client.TransactionCoordinator(id)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		idsPerCoordinator[coordinator] = append(idsPerCoordinator[coordinator], id)
	}

	for _, ids := range idsPerCoordinator {
		var response *DescribeTransactionsResponse
		err := ca.retryOnError(isRetriableTransactionCoordinatorError, func() (err error) {
			defer func() {
				if err != nil && isRetriableTransactionCoordinatorError(err) {
					for _, id := range ids {
						_ = ca.client.RefreshTransactionCoordinator(id)
					}
				}
			}()

			coordinator, err := ca.client.TransactionCoordinator(ids[0])
			if err != nil {
				return err
			}

			response, err = coordinator.DescribeTransactions(&DescribeTransactionsRequest{TransactionalIDs: ids})
			if err != nil {
				return err
			}

			// A moved or loading coordinator answers successfully but tags the
			// affected transactions with a retriable coordinator code; lift it so
			// the coordinator is refreshed and the request retried.
			for _, state := range response.TransactionStates {
				if isRetriableTransactionCoordinatorError(state.ErrorCode) {
					return state.ErrorCode
				}
			}

			return nil
		})
		if err != nil {
			errs = append(errs, err)
			continue
		}

		for _, state := range response.TransactionStates {
			result[state.TransactionalID] = state
		}
	}

	return result, errors.Join(errs...)
}

// isRetriableTransactionCoordinatorError reports whether the given error is a
// transaction-coordinator error that refreshing the coordinator and retrying can
// resolve. It mirrors the set the transaction manager treats as retriable
// (COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR, COORDINATOR_LOAD_IN_PROGRESS) plus
// EOF for a dropped connection.
func isRetriableTransactionCoordinatorError(err error) bool {
	switch {
	case errors.Is(err, ErrConsumerCoordinatorNotAvailable):
		return true
	case errors.Is(err, ErrNotCoordinatorForConsumer):
		return true
	case errors.Is(err, ErrOffsetsLoadInProgress):
		return true
	case errors.Is(err, io.EOF):
		return true
	default:
		return false
	}
}

func (ca *clusterAdmin) ListTransactions(stateFilters []string, producerIDFilters []int64, durationFilterMs int64) ([]ListTransactionsResponseTransactionState, error) {
	// Transactions may be listed by any broker, so query all brokers in parallel
	// and merge the results.
	brokers := ca.client.Brokers()
	results := make(chan []ListTransactionsResponseTransactionState, len(brokers))
	errChan := make(chan error, len(brokers))
	wg := sync.WaitGroup{}

	for _, b := range brokers {
		wg.Add(1)
		go func(b *Broker) {
			defer wg.Done()
			_ = b.Open(ca.conf) // Ensure that broker is opened

			request := &ListTransactionsRequest{
				StateFilters:      stateFilters,
				ProducerIDFilters: producerIDFilters,
				DurationFilter:    -1,
			}
			if ca.conf.Version.IsAtLeast(V3_8_0_0) {
				// Version 1 adds the DurationFilter field.
				request.Version = 1
				request.DurationFilter = durationFilterMs
			}

			response, err := b.ListTransactions(request)
			if err != nil {
				errChan <- err
				return
			}
			if !errors.Is(response.ErrorCode, ErrNoError) {
				errChan <- response.ErrorCode
				return
			}

			results <- response.TransactionStates
		}(b)
	}

	wg.Wait()
	close(results)
	close(errChan)

	var allTransactions []ListTransactionsResponseTransactionState
	for states := range results {
		allTransactions = append(allTransactions, states...)
	}

	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	return allTransactions, errors.Join(errs...)
}

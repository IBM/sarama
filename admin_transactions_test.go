//go:build !functional

package sarama

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterAdminDescribeProducers(t *testing.T) {
	topicName := "my_topic"
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	producer := ProducerState{ProducerID: 1234, ProducerEpoch: 5, LastSequence: 10}
	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetLeader(topicName, 0, seedBroker.BrokerID()).
			SetLeader(topicName, 1, seedBroker.BrokerID()),
		"DescribeProducersRequest": NewMockDescribeProducersResponse(t).
			AddProducer(topicName, 0, producer),
	})

	config := NewTestConfig()
	config.Version = V2_8_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	result, err := txAdmin.DescribeProducers(map[string][]int32{topicName: {0, 1}})
	require.NoError(t, err)
	require.Len(t, result[topicName], 2)
	require.Equal(t, int64(1234), result[topicName][0].ActiveProducers[0].ProducerID)
	require.Empty(t, result[topicName][1].ActiveProducers)
}

func TestClusterAdminDescribeProducersMultiBroker(t *testing.T) {
	topicName := "my_topic"
	seedBroker := NewMockBroker(t, 1)
	secondBroker := NewMockBroker(t, 2)
	defer seedBroker.Close()
	defer secondBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.BrokerID()).
			SetLeader(topicName, 0, seedBroker.BrokerID()).
			SetLeader(topicName, 1, secondBroker.BrokerID()),
		"DescribeProducersRequest": NewMockDescribeProducersResponse(t).
			AddProducer(topicName, 0, ProducerState{ProducerID: 1}),
	})
	secondBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.BrokerID()).
			SetLeader(topicName, 0, seedBroker.BrokerID()).
			SetLeader(topicName, 1, secondBroker.BrokerID()),
		"DescribeProducersRequest": NewMockDescribeProducersResponse(t).
			AddProducer(topicName, 1, ProducerState{ProducerID: 2}),
	})

	config := NewTestConfig()
	config.Version = V2_8_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	result, err := txAdmin.DescribeProducers(map[string][]int32{topicName: {0, 1}})
	require.NoError(t, err)
	require.Len(t, result[topicName], 2)
	require.Equal(t, int64(1), result[topicName][0].ActiveProducers[0].ProducerID)
	require.Equal(t, int64(2), result[topicName][1].ActiveProducers[0].ProducerID)
}

func TestClusterAdminDescribeProducersEmpty(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
	})

	config := NewTestConfig()
	config.Version = V2_8_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	// No DescribeProducersRequest handler is registered: if the method issued a
	// request for empty input the mock broker would error.
	result, err := txAdmin.DescribeProducers(nil)
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestClusterAdminDescribeProducersPartialFailure(t *testing.T) {
	topicName := "my_topic"
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetLeader(topicName, 0, seedBroker.BrokerID()),
		"DescribeProducersRequest": NewMockDescribeProducersResponse(t).
			AddProducer(topicName, 0, ProducerState{ProducerID: 7}),
	})

	config := NewTestConfig()
	config.Version = V2_8_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	// Partition 0 has a leader; partition 99 does not, so its leader lookup fails
	// while the reachable partition still returns a result.
	result, err := txAdmin.DescribeProducers(map[string][]int32{topicName: {0, 99}})
	require.Error(t, err)
	require.Equal(t, int64(7), result[topicName][0].ActiveProducers[0].ProducerID)
}

func TestClusterAdminDescribeProducersSurfacesPartitionError(t *testing.T) {
	topicName := "my_topic"
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetLeader(topicName, 0, seedBroker.BrokerID()),
		"DescribeProducersRequest": NewMockDescribeProducersResponse(t).
			SetError(topicName, 0, ErrClusterAuthorizationFailed),
	})

	config := NewTestConfig()
	config.Version = V2_8_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	result, err := txAdmin.DescribeProducers(map[string][]int32{topicName: {0}})
	// A per-partition error code is carried through in the struct, not lifted to
	// the aggregate error.
	require.NoError(t, err)
	require.Equal(t, ErrClusterAuthorizationFailed, result[topicName][0].ErrorCode)
}

func TestClusterAdminDescribeProducersBrokerTransportError(t *testing.T) {
	topicName := "my_topic"
	seedBroker := NewMockBroker(t, 1)
	secondBroker := NewMockBroker(t, 2)
	defer seedBroker.Close()
	secondClosed := false
	defer func() {
		if !secondClosed {
			secondBroker.Close()
		}
	}()

	metadata := NewMockMetadataResponse(t).
		SetController(seedBroker.BrokerID()).
		SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
		SetBroker(secondBroker.Addr(), secondBroker.BrokerID()).
		SetLeader(topicName, 0, seedBroker.BrokerID()).
		SetLeader(topicName, 1, secondBroker.BrokerID())
	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": metadata,
		"DescribeProducersRequest": NewMockDescribeProducersResponse(t).
			AddProducer(topicName, 0, ProducerState{ProducerID: 7}),
	})
	secondBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": metadata,
	})

	config := NewTestConfig()
	config.Version = V2_8_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	// Cache both partition leaders while the brokers are reachable, then close the
	// leader of partition 1 so its request fails at the transport layer while
	// partition 0's leader still answers.
	ca := admin.(*clusterAdmin)
	require.NoError(t, ca.client.RefreshMetadata(topicName))
	secondBroker.Close()
	secondClosed = true

	txAdmin := admin.(TransactionClusterAdmin)
	result, err := txAdmin.DescribeProducers(map[string][]int32{topicName: {0, 1}})
	// The reachable partition is returned; the unreachable partition is dropped and
	// its transport failure surfaces in the aggregate error.
	require.Error(t, err)
	require.Equal(t, int64(7), result[topicName][0].ActiveProducers[0].ProducerID)
	_, present := result[topicName][1]
	require.False(t, present)
}

func TestClusterAdminDescribeTransactions(t *testing.T) {
	txID := "my-tx"
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).
			SetCoordinator(CoordinatorTransaction, txID, seedBroker),
		"DescribeTransactionsRequest": NewMockDescribeTransactionsResponse(t).
			AddTransaction(txID, TransactionState{TransactionState: TransactionStateOngoing, ProducerID: 42}),
	})

	config := NewTestConfig()
	config.Version = V3_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	result, err := txAdmin.DescribeTransactions([]string{txID})
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, TransactionStateOngoing, result[txID].TransactionState)
	require.Equal(t, int64(42), result[txID].ProducerID)
}

func TestClusterAdminDescribeTransactionsMultiCoordinator(t *testing.T) {
	firstTx := "tx-1"
	secondTx := "tx-2"
	seedBroker := NewMockBroker(t, 1)
	secondBroker := NewMockBroker(t, 2)
	defer seedBroker.Close()
	defer secondBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.BrokerID()),
		"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).
			SetCoordinator(CoordinatorTransaction, firstTx, seedBroker).
			SetCoordinator(CoordinatorTransaction, secondTx, secondBroker),
		"DescribeTransactionsRequest": NewMockDescribeTransactionsResponse(t).
			AddTransaction(firstTx, TransactionState{TransactionState: TransactionStateOngoing}),
	})
	secondBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.BrokerID()),
		"DescribeTransactionsRequest": NewMockDescribeTransactionsResponse(t).
			AddTransaction(secondTx, TransactionState{TransactionState: TransactionStateEmpty}),
	})

	config := NewTestConfig()
	config.Version = V3_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	result, err := txAdmin.DescribeTransactions([]string{firstTx, secondTx})
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, TransactionStateOngoing, result[firstTx].TransactionState)
	require.Equal(t, TransactionStateEmpty, result[secondTx].TransactionState)
}

func TestClusterAdminDescribeTransactionsEmpty(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
	})

	config := NewTestConfig()
	config.Version = V3_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	// No FindCoordinator/DescribeTransactions handlers: if the method looked up a
	// coordinator for empty input the mock broker would error.
	result, err := txAdmin.DescribeTransactions(nil)
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestClusterAdminDescribeTransactionsRetriesOnRetriableCoordinatorError(t *testing.T) {
	for _, kerr := range []KError{ErrNotCoordinatorForConsumer, ErrConsumerCoordinatorNotAvailable, ErrOffsetsLoadInProgress} {
		t.Run(kerr.Error(), func(t *testing.T) {
			txID := "my-tx"
			seedBroker := NewMockBroker(t, 1)
			defer seedBroker.Close()

			seedBroker.SetHandlerByMap(map[string]MockResponse{
				"MetadataRequest": NewMockMetadataResponse(t).
					SetController(seedBroker.BrokerID()).
					SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
				"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).
					SetCoordinator(CoordinatorTransaction, txID, seedBroker),
				// The coordinator first answers successfully but tags the
				// transaction with a retriable coordinator code, then succeeds.
				"DescribeTransactionsRequest": NewMockSequence(
					NewMockDescribeTransactionsResponse(t).SetError(txID, kerr),
					NewMockDescribeTransactionsResponse(t).
						AddTransaction(txID, TransactionState{TransactionState: TransactionStateOngoing}),
				),
			})

			config := NewTestConfig()
			config.Version = V3_0_0_0
			config.Admin.Retry.Backoff = 0
			admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
			require.NoError(t, err)
			defer func() { _ = admin.Close() }()

			txAdmin := admin.(TransactionClusterAdmin)
			result, err := txAdmin.DescribeTransactions([]string{txID})
			require.NoError(t, err)
			require.Equal(t, TransactionStateOngoing, result[txID].TransactionState)
			require.Equal(t, ErrNoError, result[txID].ErrorCode)
		})
	}
}

func TestClusterAdminDescribeTransactionsRegroupsCoordinatorsOnRetry(t *testing.T) {
	firstTx := "tx-1"
	secondTx := "tx-2"
	seedBroker := NewMockBroker(t, 1)
	secondBroker := NewMockBroker(t, 2)
	defer seedBroker.Close()
	defer secondBroker.Close()

	metadata := NewMockMetadataResponse(t).
		SetController(seedBroker.BrokerID()).
		SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
		SetBroker(secondBroker.Addr(), secondBroker.BrokerID())

	// Both transactions initially resolve to the seed broker, so they are grouped
	// into a single request. The initial grouping issues exactly one
	// FindCoordinator per id (both -> seed), consuming the first two sequence
	// entries; every refresh afterwards sees tx-2 moved to the second broker.
	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": metadata,
		"FindCoordinatorRequest": NewMockSequence(
			NewMockFindCoordinatorResponse(t).
				SetCoordinator(CoordinatorTransaction, firstTx, seedBroker).
				SetCoordinator(CoordinatorTransaction, secondTx, seedBroker),
			NewMockFindCoordinatorResponse(t).
				SetCoordinator(CoordinatorTransaction, firstTx, seedBroker).
				SetCoordinator(CoordinatorTransaction, secondTx, seedBroker),
			NewMockFindCoordinatorResponse(t).
				SetCoordinator(CoordinatorTransaction, firstTx, seedBroker).
				SetCoordinator(CoordinatorTransaction, secondTx, secondBroker),
		),
		// The seed broker owns tx-1 but not tx-2, which it rejects with
		// NOT_COORDINATOR on every attempt. The pre-fix code sent the whole group
		// to ids[0]'s coordinator on retry, so tx-2 could never succeed here.
		"DescribeTransactionsRequest": NewMockDescribeTransactionsResponse(t).
			AddTransaction(firstTx, TransactionState{TransactionState: TransactionStateOngoing}).
			SetError(secondTx, ErrNotCoordinatorForConsumer),
	})
	secondBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": metadata,
		"DescribeTransactionsRequest": NewMockDescribeTransactionsResponse(t).
			AddTransaction(secondTx, TransactionState{TransactionState: TransactionStateEmpty}),
	})

	config := NewTestConfig()
	config.Version = V3_0_0_0
	config.Admin.Retry.Backoff = 0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	result, err := txAdmin.DescribeTransactions([]string{firstTx, secondTx})
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, TransactionStateOngoing, result[firstTx].TransactionState)
	require.Equal(t, TransactionStateEmpty, result[secondTx].TransactionState)
}

func TestClusterAdminDescribeTransactionsPartialFailure(t *testing.T) {
	okTx := "ok-tx"
	badTx := "bad-tx"
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).
			SetCoordinator(CoordinatorTransaction, okTx, seedBroker).
			SetError(CoordinatorTransaction, badTx, ErrClusterAuthorizationFailed),
		"DescribeTransactionsRequest": NewMockDescribeTransactionsResponse(t).
			AddTransaction(okTx, TransactionState{TransactionState: TransactionStateOngoing}),
	})

	config := NewTestConfig()
	config.Version = V3_0_0_0
	config.Admin.Retry.Backoff = 0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	result, err := txAdmin.DescribeTransactions([]string{okTx, badTx})
	require.Error(t, err)
	require.Equal(t, TransactionStateOngoing, result[okTx].TransactionState)
	_, present := result[badTx]
	require.False(t, present)
}

func TestClusterAdminDescribeTransactionsSurfacesTerminalError(t *testing.T) {
	txID := "my-tx"
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).
			SetCoordinator(CoordinatorTransaction, txID, seedBroker),
		"DescribeTransactionsRequest": NewMockDescribeTransactionsResponse(t).
			SetError(txID, ErrTransactionalIDAuthorizationFailed),
	})

	config := NewTestConfig()
	config.Version = V3_0_0_0
	config.Admin.Retry.Backoff = 0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	result, err := txAdmin.DescribeTransactions([]string{txID})
	// A terminal per-transaction code is carried through in the struct, not lifted
	// to the aggregate error and not retried.
	require.NoError(t, err)
	require.Equal(t, ErrTransactionalIDAuthorizationFailed, result[txID].ErrorCode)
}

func TestClusterAdminDescribeTransactionsExhaustsRetries(t *testing.T) {
	txID := "my-tx"
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).
			SetCoordinator(CoordinatorTransaction, txID, seedBroker),
		// The coordinator tags the transaction with a retriable code on every
		// attempt, so refreshing and retrying never clears it.
		"DescribeTransactionsRequest": NewMockDescribeTransactionsResponse(t).
			SetError(txID, ErrNotCoordinatorForConsumer),
	})

	config := NewTestConfig()
	config.Version = V3_0_0_0
	config.Admin.Retry.Max = 2
	config.Admin.Retry.Backoff = 0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	result, err := txAdmin.DescribeTransactions([]string{txID})
	// Retries are bounded: once exhausted the retriable code is surfaced as the
	// aggregate error and the transaction is left out of the result.
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotCoordinatorForConsumer)
	_, present := result[txID]
	require.False(t, present)
}

func TestIsRetriableTransactionCoordinatorError(t *testing.T) {
	// The full "coordinator dropped an established connection" path cannot be
	// reproduced deterministically with MockBroker, so the io.EOF branch is
	// pinned here at the predicate level; the retry machinery that consumes it is
	// exercised by TestClusterAdminDescribeTransactionsRetriesOnRetriableCoordinatorError.
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"coordinator not available", ErrConsumerCoordinatorNotAvailable, true},
		{"not coordinator", ErrNotCoordinatorForConsumer, true},
		{"coordinator load in progress", ErrOffsetsLoadInProgress, true},
		{"dropped connection", io.EOF, true},
		{"wrapped dropped connection", fmt.Errorf("reading response: %w", io.EOF), true},
		{"terminal authorization error", ErrTransactionalIDAuthorizationFailed, false},
		{"unrelated error", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isRetriableTransactionCoordinatorError(tt.err))
		})
	}
}

func TestClusterAdminListTransactions(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"ListTransactionsRequest": NewMockListTransactionsResponse(t).
			AddTransaction("tx-1", 100, TransactionStateOngoing),
	})

	config := NewTestConfig()
	config.Version = V3_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	result, err := txAdmin.ListTransactions(nil, nil, -1)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, "tx-1", result[0].TransactionalID)
	require.Equal(t, int64(100), result[0].ProducerID)
	require.Equal(t, TransactionStateOngoing, result[0].TransactionState)
}

func TestClusterAdminListTransactionsMultiBroker(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	secondBroker := NewMockBroker(t, 2)
	defer seedBroker.Close()
	defer secondBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.BrokerID()),
		"ListTransactionsRequest": NewMockListTransactionsResponse(t).
			AddTransaction("tx-1", 1, TransactionStateOngoing),
	})
	secondBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.BrokerID()),
		"ListTransactionsRequest": NewMockListTransactionsResponse(t).
			AddTransaction("tx-2", 2, TransactionStateEmpty),
	})

	config := NewTestConfig()
	config.Version = V3_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	result, err := txAdmin.ListTransactions(nil, nil, -1)
	require.NoError(t, err)
	require.Len(t, result, 2)
	ids := map[string]bool{}
	for _, s := range result {
		ids[s.TransactionalID] = true
	}
	require.True(t, ids["tx-1"])
	require.True(t, ids["tx-2"])
}

func TestClusterAdminListTransactionsUsesV1WithDurationFilter(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	mock := NewMockListTransactionsResponse(t).AddTransaction("tx-1", 1, TransactionStateOngoing)
	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"ListTransactionsRequest": mock,
	})

	config := NewTestConfig()
	config.Version = V3_8_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	_, err = txAdmin.ListTransactions([]string{TransactionStateOngoing}, []int64{7}, 5000)
	require.NoError(t, err)

	req := mock.LastRequest()
	require.Equal(t, int16(1), req.Version)
	require.Equal(t, int64(5000), req.DurationFilter)
	require.Equal(t, []string{TransactionStateOngoing}, req.StateFilters)
	require.Equal(t, []int64{7}, req.ProducerIDFilters)
}

func TestClusterAdminListTransactionsUsesV0BelowV3_8(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	mock := NewMockListTransactionsResponse(t)
	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"ListTransactionsRequest": mock,
	})

	config := NewTestConfig()
	config.Version = V3_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	// No duration filter is requested (disabled), so a v0 request is sent and the
	// duration filter never reaches the wire.
	_, err = txAdmin.ListTransactions(nil, nil, -1)
	require.NoError(t, err)

	req := mock.LastRequest()
	require.Equal(t, int16(0), req.Version)
	require.Equal(t, int64(-1), req.DurationFilter)
}

func TestClusterAdminListTransactionsRejectsDurationFilterBelowV3_8(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
	})

	config := NewTestConfig()
	config.Version = V3_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	// A real duration filter needs ListTransactions v1 (Kafka 3.8.0.0+); below
	// that it must be rejected rather than silently ignored.
	result, err := txAdmin.ListTransactions(nil, nil, 5000)
	require.Nil(t, result)
	var configErr ConfigurationError
	require.ErrorAs(t, err, &configErr)
}

func TestClusterAdminListTransactionsPartialFailure(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	secondBroker := NewMockBroker(t, 2)
	defer seedBroker.Close()
	defer secondBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.BrokerID()),
		"ListTransactionsRequest": NewMockListTransactionsResponse(t).
			AddTransaction("tx-1", 1, TransactionStateOngoing),
	})
	secondBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(secondBroker.Addr(), secondBroker.BrokerID()),
		"ListTransactionsRequest": NewMockListTransactionsResponse(t).
			SetError(ErrClusterAuthorizationFailed),
	})

	config := NewTestConfig()
	config.Version = V3_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	result, err := txAdmin.ListTransactions(nil, nil, -1)
	require.Error(t, err)
	require.Len(t, result, 1)
	require.Equal(t, "tx-1", result[0].TransactionalID)
}

func TestClusterAdminListTransactionsSurfacesTopLevelError(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"ListTransactionsRequest": NewMockListTransactionsResponse(t).
			SetError(ErrClusterAuthorizationFailed),
	})

	config := NewTestConfig()
	config.Version = V3_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	txAdmin := admin.(TransactionClusterAdmin)
	result, err := txAdmin.ListTransactions(nil, nil, -1)
	require.Error(t, err)
	require.Empty(t, result)
}

func TestClusterAdminImplementsTransactionClusterAdmin(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
	})

	config := NewTestConfig()
	config.Version = V3_0_0_0
	admin, err := NewClusterAdmin([]string{seedBroker.Addr()}, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	_, ok := admin.(TransactionClusterAdmin)
	require.True(t, ok, "NewClusterAdmin result should implement TransactionClusterAdmin")
}

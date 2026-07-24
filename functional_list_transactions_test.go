//go:build functional

package sarama

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFuncListTransactions(t *testing.T) {
	checkKafkaVersion(t, "3.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "TestFuncListTransactions"
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Return.Successes = true
	config.Net.MaxOpenRequests = 1

	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer safeClose(t, client)

	// a transactional producer registers its transactional id with the
	// transaction coordinator on startup via InitProducerId
	producer, err := NewAsyncProducerFromClient(client)
	require.NoError(t, err)
	defer safeClose(t, producer)

	coordinator, err := client.TransactionCoordinator(config.Producer.Transaction.ID)
	require.NoError(t, err)

	res, err := coordinator.ListTransactions(NewListTransactionsRequest(config.Version))
	require.NoError(t, err)
	require.Equal(t, ErrNoError, res.ErrorCode)
	assert.Empty(t, res.UnknownStateFilters)

	ids := make([]string, 0, len(res.TransactionStates))
	for _, state := range res.TransactionStates {
		ids = append(ids, state.TransactionalID)
	}
	assert.Contains(t, ids, config.Producer.Transaction.ID)
}

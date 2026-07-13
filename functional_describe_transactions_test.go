//go:build functional

package sarama

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFuncDescribeTransactions(t *testing.T) {
	checkKafkaVersion(t, "3.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "TestFuncDescribeTransactions"
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

	req := &DescribeTransactionsRequest{
		TransactionalIDs: []string{config.Producer.Transaction.ID},
	}
	res, err := coordinator.DescribeTransactions(req)
	require.NoError(t, err)

	require.Len(t, res.TransactionStates, 1)

	state := res.TransactionStates[0]
	require.Equal(t, ErrNoError, state.ErrorCode)
	assert.Equal(t, config.Producer.Transaction.ID, state.TransactionalID)
	assert.NotEmpty(t, state.TransactionState)
	assert.GreaterOrEqual(t, state.ProducerID, int64(0))
}

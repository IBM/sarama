//go:build !functional

package sarama

import (
	"testing"
	"time"
)

// v1 has the same wire format as v0
var listTransactionsResponseV0 = []byte{
	0, 0, 0, 100, // throttleTimeMs
	0, 0, // error code
	2,                // UnknownStateFilters (one element array)
	4, 'f', 'o', 'o', // unknown state filter
	2,                // TransactionStates (one element array)
	4, 't', 'x', 'n', // transactional id
	0, 0, 0, 0, 0, 0, 3, 232, // producer id
	8, 'O', 'n', 'g', 'o', 'i', 'n', 'g', // transaction state
	0, // empty tagged fields
	0, // empty tagged fields
}

func TestListTransactionsResponse(t *testing.T) {
	response := &ListTransactionsResponse{
		Version:             0,
		ThrottleTime:        100 * time.Millisecond,
		ErrorCode:           ErrNoError,
		UnknownStateFilters: []string{"foo"},
		TransactionStates: []ListTransactionsResponseTransactionState{
			{
				TransactionalID:  "txn",
				ProducerID:       1000,
				TransactionState: TransactionStateOngoing,
			},
		},
	}

	testResponse(t, "v0", response, listTransactionsResponseV0)

	response.Version = 1
	testResponse(t, "v1", response, listTransactionsResponseV0)
}

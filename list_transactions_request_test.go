//go:build !functional

package sarama

import (
	"testing"
)

var listTransactionsRequestV0 = []byte{
	2,                                    // StateFilters (one element array)
	8, 'O', 'n', 'g', 'o', 'i', 'n', 'g', // state filter
	2,                        // ProducerIdFilters (one element array)
	0, 0, 0, 0, 0, 0, 3, 232, // producer id filter
	0, // empty tagged fields
}

var listTransactionsRequestV1 = []byte{
	2,                                    // StateFilters (one element array)
	8, 'O', 'n', 'g', 'o', 'i', 'n', 'g', // state filter
	2,                        // ProducerIdFilters (one element array)
	0, 0, 0, 0, 0, 0, 3, 232, // producer id filter
	0, 0, 0, 0, 0, 0, 0, 100, // duration filter ms
	0, // empty tagged fields
}

func TestListTransactionsRequest(t *testing.T) {
	request := &ListTransactionsRequest{
		Version:           0,
		StateFilters:      []string{TransactionStateOngoing},
		ProducerIDFilters: []int64{1000},
		DurationFilter:    -1,
	}

	testRequest(t, "v0", request, listTransactionsRequestV0)

	request.Version = 1
	request.DurationFilter = 100
	testRequest(t, "v1", request, listTransactionsRequestV1)
}

//go:build !functional

package sarama

import (
	"testing"
)

var describeTransactionsRequestV0 = []byte{
	2,                // TransactionalIds (one element array)
	4, 't', 'x', 'n', // transactional id
	0, // empty tagged fields
}

func TestDescribeTransactionsRequest(t *testing.T) {
	request := &DescribeTransactionsRequest{
		Version:          0,
		TransactionalIDs: []string{"txn"},
	}

	testRequest(t, "v0", request, describeTransactionsRequestV0)
}

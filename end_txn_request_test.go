//go:build !functional

package sarama

import "testing"

var (
	endTxnRequest = []byte{
		0, 3, 't', 'x', 'n',
		0, 0, 0, 0, 0, 0, 31, 64,
		0, 1,
		1,
	}

	endTxnRequestV3 = []byte{
		4, 't', 'x', 'n', // TransactionalId
		0, 0, 0, 0, 0, 0, 31, 64, // ProducerId
		0, 1, // ProducerEpoch
		1, // Committed
		0, // tagged fields
	}
)

func TestEndTxnRequest(t *testing.T) {
	req := &EndTxnRequest{
		TransactionalID:   "txn",
		ProducerID:        8000,
		ProducerEpoch:     1,
		TransactionResult: true,
	}

	testRequest(t, "v0", req, endTxnRequest)

	req.Version = 3
	testRequest(t, "v3", req, endTxnRequestV3)
}

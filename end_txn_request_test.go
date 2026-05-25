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
		0x04, 't', 'x', 'n',
		0, 0, 0, 0, 0, 0, 31, 64,
		0, 1,
		1,
		0x00, // empty tagged fields
	}
)

func TestEndTxnRequest(t *testing.T) {
	req := &EndTxnRequest{
		TransactionalID:   "txn",
		ProducerID:        8000,
		ProducerEpoch:     1,
		TransactionResult: true,
	}

	testRequest(t, "", req, endTxnRequest)
}

func TestEndTxnRequestV3(t *testing.T) {
	req := &EndTxnRequest{
		Version:           3,
		TransactionalID:   "txn",
		ProducerID:        8000,
		ProducerEpoch:     1,
		TransactionResult: true,
	}

	testRequest(t, "v3", req, endTxnRequestV3)
}

func TestEndTxnRequestV5(t *testing.T) {
	req := &EndTxnRequest{
		Version:           5,
		TransactionalID:   "txn",
		ProducerID:        8000,
		ProducerEpoch:     1,
		TransactionResult: true,
	}

	testRequestWithoutByteComparison(t, "v5", req)
}

//go:build !functional

package sarama

import "testing"

var (
	addPartitionsToTxnRequest = []byte{
		0, 3, 't', 'x', 'n',
		0, 0, 0, 0, 0, 0, 31, 64, // ProducerID
		0, 0, 0, 0, // ProducerEpoch
		0, 1, // 1 topic
		0, 5, 't', 'o', 'p', 'i', 'c',
		0, 0, 0, 1, 0, 0, 0, 1,
	}

	addPartitionsToTxnRequestV3 = []byte{
		4, 't', 'x', 'n',
		0, 0, 0, 0, 0, 0, 31, 64,
		0, 0,
		2,
		6, 't', 'o', 'p', 'i', 'c',
		2,
		0, 0, 0, 1,
		0, // empty tagged fields on topic entry
		0, // empty tagged fields on request
	}
)

func TestAddPartitionsToTxnRequest(t *testing.T) {
	req := &AddPartitionsToTxnRequest{
		TransactionalID: "txn",
		ProducerID:      8000,
		ProducerEpoch:   0,
		TopicPartitions: map[string][]int32{
			"topic": {1},
		},
	}

	testRequest(t, "", req, addPartitionsToTxnRequest)
}

func TestAddPartitionsToTxnRequestV3(t *testing.T) {
	req := &AddPartitionsToTxnRequest{
		Version:         3,
		TransactionalID: "txn",
		ProducerID:      8000,
		ProducerEpoch:   0,
		TopicPartitions: map[string][]int32{
			"topic": {1},
		},
	}

	testRequest(t, "v3", req, addPartitionsToTxnRequestV3)
}

func TestAddPartitionsToTxnRequestV4Batching(t *testing.T) {
	req := &AddPartitionsToTxnRequest{
		Version: 4,
		Transactions: []AddPartitionsToTxnTransaction{
			{
				TransactionalID: "txn",
				ProducerID:      8000,
				ProducerEpoch:   0,
				TopicPartitions: map[string][]int32{
					"topic": {1},
				},
			},
		},
	}

	testRequestWithoutByteComparison(t, "v4", req)
}

func TestAddPartitionsToTxnRequestV5VerifyOnly(t *testing.T) {
	req := &AddPartitionsToTxnRequest{
		Version: 5,
		Transactions: []AddPartitionsToTxnTransaction{
			{
				TransactionalID: "txn",
				ProducerID:      8000,
				ProducerEpoch:   0,
				VerifyOnly:      true,
				TopicPartitions: map[string][]int32{
					"topic": {1},
				},
			},
		},
	}

	testRequestWithoutByteComparison(t, "v5 verify-only", req)
}

//go:build !functional

package sarama

import "testing"

var (
	addOffsetsToTxnRequest = []byte{
		0, 3, 't', 'x', 'n',
		0, 0, 0, 0, 0, 0, 31, 64,
		0, 0,
		0, 7, 'g', 'r', 'o', 'u', 'p', 'i', 'd',
	}

	addOffsetsToTxnRequestV3 = []byte{
		4, 't', 'x', 'n', // TransactionalId
		0, 0, 0, 0, 0, 0, 31, 64, // ProducerId
		0, 0, // ProducerEpoch
		8, 'g', 'r', 'o', 'u', 'p', 'i', 'd', // GroupId
		0, // tagged fields
	}
)

func TestAddOffsetsToTxnRequest(t *testing.T) {
	req := &AddOffsetsToTxnRequest{
		TransactionalID: "txn",
		ProducerID:      8000,
		ProducerEpoch:   0,
		GroupID:         "groupid",
	}

	testRequest(t, "v0", req, addOffsetsToTxnRequest)

	req.Version = 3
	testRequest(t, "v3", req, addOffsetsToTxnRequestV3)
}

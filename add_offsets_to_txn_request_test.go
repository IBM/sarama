package sarama

import (
	"testing"

	"go.uber.org/goleak"
)

var addOffsetsToTxnRequest = []byte{
	0, 3, 't', 'x', 'n',
	0, 0, 0, 0, 0, 0, 31, 64,
	0, 0,
	0, 7, 'g', 'r', 'o', 'u', 'p', 'i', 'd',
}

func TestAddOffsetsToTxnRequest(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	req := &AddOffsetsToTxnRequest{
		TransactionalID: "txn",
		ProducerID:      8000,
		ProducerEpoch:   0,
		GroupID:         "groupid",
	}

	testRequest(t, "", req, addOffsetsToTxnRequest)
}

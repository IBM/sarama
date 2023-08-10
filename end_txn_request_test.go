package sarama

import (
	"testing"

	"go.uber.org/goleak"
)

var endTxnRequest = []byte{
	0, 3, 't', 'x', 'n',
	0, 0, 0, 0, 0, 0, 31, 64,
	0, 1,
	1,
}

func TestEndTxnRequest(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	req := &EndTxnRequest{
		TransactionalID:   "txn",
		ProducerID:        8000,
		ProducerEpoch:     1,
		TransactionResult: true,
	}

	testRequest(t, "", req, endTxnRequest)
}

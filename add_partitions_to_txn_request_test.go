package sarama

import (
	"testing"

	"go.uber.org/goleak"
)

var addPartitionsToTxnRequest = []byte{
	0, 3, 't', 'x', 'n',
	0, 0, 0, 0, 0, 0, 31, 64, // ProducerID
	0, 0, 0, 0, // ProducerEpoch
	0, 1, // 1 topic
	0, 5, 't', 'o', 'p', 'i', 'c',
	0, 0, 0, 1, 0, 0, 0, 1,
}

func TestAddPartitionsToTxnRequest(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
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

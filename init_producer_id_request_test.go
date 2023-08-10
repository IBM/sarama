package sarama

import (
	"testing"
	"time"

	"go.uber.org/goleak"
)

var (
	initProducerIDRequestNull = []byte{
		255, 255,
		0, 0, 0, 100,
	}

	initProducerIDRequest = []byte{
		0, 3, 't', 'x', 'n',
		0, 0, 0, 100,
	}

	initProducerIDRequestTaggedFields = []byte{
		4, 116, 120, 110, // TransactionID in compact string
		0, 0, 0, 100, // TransactionTimeout
		0, // empty TaggedFields
	}

	initProducerIDRequestProducerId = []byte{
		4, 116, 120, 110, // TransactionID in compact string
		0, 0, 0, 100, // TransactionTimeout
		0, 0, 0, 0, 0, 0, 0, 123, // ProducerID
		1, 65, // ProducerEpoch
		0, // empty TaggedFields
	}
)

func TestInitProducerIDRequest(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	req := &InitProducerIDRequest{
		TransactionTimeout: 100 * time.Millisecond,
	}

	testRequest(t, "null transaction id", req, initProducerIDRequestNull)

	transactionID := "txn"
	req.TransactionalID = &transactionID

	testRequest(t, "transaction id", req, initProducerIDRequest)

	req.Version = 2
	testRequest(t, "tagged fields", req, initProducerIDRequestTaggedFields)

	req.Version = 3
	req.ProducerID = 123
	req.ProducerEpoch = 321

	testRequest(t, "producer id", req, initProducerIDRequestProducerId)
}

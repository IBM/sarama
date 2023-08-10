package sarama

import (
	"testing"
	"time"

	"go.uber.org/goleak"
)

var (
	initProducerIDResponse = []byte{
		0, 0, 0, 100,
		0, 0,
		0, 0, 0, 0, 0, 0, 31, 64, // producerID = 8000
		0, 0, // epoch
	}

	initProducerIDRequestError = []byte{
		0, 0, 0, 100,
		0, 51,
		255, 255, 255, 255, 255, 255, 255, 255,
		0, 0,
	}

	initProducerIdResponseWithTaggedFields = []byte{
		0, 0, 0, 100,
		0, 51,
		255, 255, 255, 255, 255, 255, 255, 255,
		0, 0,
		0,
	}
)

func TestInitProducerIDResponse(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	resp := &InitProducerIDResponse{
		ThrottleTime:  100 * time.Millisecond,
		ProducerID:    8000,
		ProducerEpoch: 0,
	}

	testResponse(t, "", resp, initProducerIDResponse)

	resp.Err = ErrConcurrentTransactions
	resp.ProducerID = -1

	testResponse(t, "with error", resp, initProducerIDRequestError)

	resp.Version = 2
	testResponse(t, "with tagged fields", resp, initProducerIdResponseWithTaggedFields)
}

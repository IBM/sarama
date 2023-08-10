package sarama

import (
	"testing"
	"time"

	"go.uber.org/goleak"
)

var endTxnResponse = []byte{
	0, 0, 0, 100,
	0, 49,
}

func TestEndTxnResponse(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	resp := &EndTxnResponse{
		ThrottleTime: 100 * time.Millisecond,
		Err:          ErrInvalidProducerIDMapping,
	}

	testResponse(t, "", resp, endTxnResponse)
}

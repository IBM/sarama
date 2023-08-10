package sarama

import (
	"testing"
	"time"

	"go.uber.org/goleak"
)

var addOffsetsToTxnResponse = []byte{
	0, 0, 0, 100,
	0, 47,
}

func TestAddOffsetsToTxnResponse(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	resp := &AddOffsetsToTxnResponse{
		ThrottleTime: 100 * time.Millisecond,
		Err:          ErrInvalidProducerEpoch,
	}

	testResponse(t, "", resp, addOffsetsToTxnResponse)
}

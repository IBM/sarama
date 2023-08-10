package sarama

import (
	"testing"
	"time"

	"go.uber.org/goleak"
)

var addPartitionsToTxnResponse = []byte{
	0, 0, 0, 100,
	0, 0, 0, 1,
	0, 5, 't', 'o', 'p', 'i', 'c',
	0, 0, 0, 1, // 1 partition error
	0, 0, 0, 2, // partition 2
	0, 48, // error
}

func TestAddPartitionsToTxnResponse(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	resp := &AddPartitionsToTxnResponse{
		ThrottleTime: 100 * time.Millisecond,
		Errors: map[string][]*PartitionError{
			"topic": {{
				Err:       ErrInvalidTxnState,
				Partition: 2,
			}},
		},
	}

	testResponse(t, "", resp, addPartitionsToTxnResponse)
}

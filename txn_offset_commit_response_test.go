package sarama

import (
	"testing"
	"time"

	"go.uber.org/goleak"
)

var txnOffsetCommitResponse = []byte{
	0, 0, 0, 100,
	0, 0, 0, 1, // 1 topic
	0, 5, 't', 'o', 'p', 'i', 'c',
	0, 0, 0, 1, // 1 partition response
	0, 0, 0, 2, // partition number 2
	0, 47, // err
}

func TestTxnOffsetCommitResponse(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	resp := &TxnOffsetCommitResponse{
		ThrottleTime: 100 * time.Millisecond,
		Topics: map[string][]*PartitionError{
			"topic": {{
				Partition: 2,
				Err:       ErrInvalidProducerEpoch,
			}},
		},
	}

	testResponse(t, "", resp, txnOffsetCommitResponse)
}

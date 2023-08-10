package sarama

import (
	"testing"
	"time"

	"go.uber.org/goleak"
)

var deleteRecordsResponse = []byte{
	0, 0, 0, 100,
	0, 0, 0, 2,
	0, 5, 'o', 't', 'h', 'e', 'r',
	0, 0, 0, 0,
	0, 5, 't', 'o', 'p', 'i', 'c',
	0, 0, 0, 2,
	0, 0, 0, 19,
	0, 0, 0, 0, 0, 0, 0, 200,
	0, 0,
	0, 0, 0, 20,
	255, 255, 255, 255, 255, 255, 255, 255,
	0, 3,
}

func TestDeleteRecordsResponse(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	resp := &DeleteRecordsResponse{
		Version:      0,
		ThrottleTime: 100 * time.Millisecond,
		Topics: map[string]*DeleteRecordsResponseTopic{
			"topic": {
				Partitions: map[int32]*DeleteRecordsResponsePartition{
					19: {LowWatermark: 200, Err: 0},
					20: {LowWatermark: -1, Err: 3},
				},
			},
			"other": {},
		},
	}

	testResponse(t, "", resp, deleteRecordsResponse)
}

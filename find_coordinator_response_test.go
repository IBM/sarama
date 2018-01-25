package sarama

import (
	"testing"
	"time"
)

var (
	findCoordinatorResponse = []byte{
		0, 0, 0, 100,
		0, 0,
		255, 255, // empty ErrMsg
		0, 0, 0, 1,
		0, 4, 'h', 'o', 's', 't',
		0, 0, 35, 132,
	}

	findCoordinatorResponseError = []byte{
		0, 0, 0, 100,
		0, 15,
		0, 3, 'm', 's', 'g',
		0, 0, 0, 1,
		0, 4, 'h', 'o', 's', 't',
		0, 0, 35, 132,
	}
)

func TestFindCoordinatorResponse(t *testing.T) {
	broker := NewBroker("host:9092")
	broker.id = 1
	resp := &FindCoordinatorResponse{
		Version:      1,
		ThrottleTime: 100 * time.Millisecond,
		Err:          ErrNoError,
		ErrMsg:       nil,
		Coordinator:  broker,
	}

	testResponse(t, "version 1 - no error", resp, findCoordinatorResponse)

	msg := "msg"
	resp.Err = ErrConsumerCoordinatorNotAvailable
	resp.ErrMsg = &msg

	testResponse(t, "version 1 - error", resp, findCoordinatorResponseError)
}

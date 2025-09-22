//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var (
	deleteTopicsResponseV0 = []byte{
		0, 0, 0, 1,
		0, 5, 't', 'o', 'p', 'i', 'c',
		0, 0,
	}

	deleteTopicsResponseV1 = []byte{
		0, 0, 0, 100,
		0, 0, 0, 1,
		0, 5, 't', 'o', 'p', 'i', 'c',
		0, 0,
	}

	deleteTopicsResponseV4 = []byte{
		0, 0, 0, 100,
		2,
		6, 't', 'o', 'p', 'i', 'c',
		0, 0,
		0, // empty tagged fields
		0, // empty tagged fields
	}
)

func TestDeleteTopicsResponse(t *testing.T) {
	resp := &DeleteTopicsResponse{
		TopicErrorCodes: map[string]KError{
			"topic": ErrNoError,
		},
	}

	testResponse(t, "version 0", resp, deleteTopicsResponseV0)

	resp.Version = 1
	resp.ThrottleTime = 100 * time.Millisecond

	testResponse(t, "version 1", resp, deleteTopicsResponseV1)

	resp.Version = 4
	testResponse(t, "version 4", resp, deleteTopicsResponseV4)
}

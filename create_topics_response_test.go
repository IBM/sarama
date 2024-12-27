//go:build !functional

package sarama

import (
	"errors"
	"testing"
	"time"
)

var (
	createTopicsResponseV0 = []byte{
		0, 0, 0, 1,
		0, 5, 't', 'o', 'p', 'i', 'c',
		0, 42,
	}

	createTopicsResponseV1 = []byte{
		0, 0, 0, 1,
		0, 5, 't', 'o', 'p', 'i', 'c',
		0, 42,
		0, 3, 'm', 's', 'g',
	}

	createTopicsResponseV2 = []byte{
		0, 0, 0, 100,
		0, 0, 0, 1,
		0, 5, 't', 'o', 'p', 'i', 'c',
		0, 42,
		0, 3, 'm', 's', 'g',
	}
)

func TestCreateTopicsResponse(t *testing.T) {
	resp := &CreateTopicsResponse{
		TopicErrors: map[string]*TopicError{
			"topic": {
				Err: ErrInvalidRequest,
			},
		},
	}

	testResponse(t, "version 0", resp, createTopicsResponseV0)

	resp.Version = 1
	msg := "msg"
	resp.TopicErrors["topic"].ErrMsg = &msg

	testResponse(t, "version 1", resp, createTopicsResponseV1)

	resp.Version = 2
	resp.ThrottleTime = 100 * time.Millisecond

	testResponse(t, "version 2", resp, createTopicsResponseV2)
}

func TestTopicError(t *testing.T) {
	// Assert that TopicError satisfies error interface
	var err error = &TopicError{
		Err: ErrTopicAuthorizationFailed,
	}

	if !errors.Is(err, ErrTopicAuthorizationFailed) {
		t.Errorf("unexpected errors.Is")
	}

	got := err.Error()
	want := ErrTopicAuthorizationFailed.Error()
	if got != want {
		t.Errorf("TopicError.Error() = %v; want %v", got, want)
	}

	msg := "reason why topic authorization failed"
	err = &TopicError{
		Err:    ErrTopicAuthorizationFailed,
		ErrMsg: &msg,
	}
	got = err.Error()
	want = ErrTopicAuthorizationFailed.Error() + " - " + msg
	if got != want {
		t.Errorf("TopicError.Error() = %v; want %v", got, want)
	}
}

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

	createTopicsResponseV5 = []byte{
		0, 0, 0, 100,
		2,
		6, 't', 'o', 'p', 'i', 'c',
		0, 42, // invalid request error
		4, 'm', 's', 'g', // error message
		0, 0, 0, 1, // num partitions
		0, 2, // replication factor
		2,                // 1 config
		4, 'b', 'a', 'r', // name
		4, 'b', 'a', 'z', // value
		0, // read only
		5, // source default
		0, // is sensitive
		0, // empty tagged fields
		0, // empty tagged fields
		0, // empty tagged fields
	}

	createTopicsResponseV5WithTopicConfigError = []byte{
		0, 0, 0, 100,
		2,
		6, 't', 'o', 'p', 'i', 'c',
		0, 42, // invalid request error
		4, 'm', 's', 'g', // error message
		0, 0, 0, 1, // num partitions
		0, 2, // replication factor
		2,                // 1 config
		4, 'b', 'a', 'r', // name
		4, 'b', 'a', 'z', // value
		0,     // read only
		5,     // source default
		0,     // is sensitive
		0,     // empty tagged fields
		1,     // one tagged field
		0,     // tag identifier
		2,     // 2 length of data
		0, 29, // TOPIC_AUTHORIZATION_FAILED error (see KIP-525)
		0, // empty tagged fields
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

	resp.Version = 5
	resp.TopicResults = map[string]*CreatableTopicResult{
		"topic": {
			NumPartitions:     1,
			ReplicationFactor: 2,
			Configs: map[string]*CreatableTopicConfigs{
				"bar": {
					Value:        nullString("baz"),
					ConfigSource: SourceDefault,
				},
			},
		},
	}
	testResponse(t, "version 5", resp, createTopicsResponseV5)

	resp.TopicResults["topic"].TopicConfigErrorCode = ErrTopicAuthorizationFailed
	testResponse(t, "version 5", resp, createTopicsResponseV5WithTopicConfigError)
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

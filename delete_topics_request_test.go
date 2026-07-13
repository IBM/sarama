//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var (
	deleteTopicsRequest = []byte{
		0, 0, 0, 2,
		0, 5, 't', 'o', 'p', 'i', 'c',
		0, 5, 'o', 't', 'h', 'e', 'r',
		0, 0, 0, 100,
	}
	deleteTopicsRequestV4 = []byte{
		3,
		6, 't', 'o', 'p', 'i', 'c',
		6, 'o', 't', 'h', 'e', 'r',
		0, 0, 0, 100,
		0, // empty tagged fields
	}

	deleteTopicsRequestV5 = deleteTopicsRequestV4

	deleteTopicsRequestV6 = []byte{
		3,                          // Topics
		6, 't', 'o', 'p', 'i', 'c', // Name
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // TopicId
		0,                          // tagged fields (topic)
		6, 'o', 't', 'h', 'e', 'r', // Name
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // TopicId
		0,            // tagged fields (topic)
		0, 0, 0, 100, // TimeoutMs
		0, // tagged fields
	}
)

func TestDeleteTopicsRequestV0(t *testing.T) {
	req := &DeleteTopicsRequest{
		Version: 0,
		Topics:  []string{"topic", "other"},
		Timeout: 100 * time.Millisecond,
	}

	testRequest(t, "", req, deleteTopicsRequest)
}

func TestDeleteTopicsRequestV1(t *testing.T) {
	req := &DeleteTopicsRequest{
		Version: 1,
		Topics:  []string{"topic", "other"},
		Timeout: 100 * time.Millisecond,
	}

	testRequest(t, "", req, deleteTopicsRequest)
}

func TestDeleteTopicsRequestV4(t *testing.T) {
	req := &DeleteTopicsRequest{
		Version: 4,
		Topics:  []string{"topic", "other"},
		Timeout: 100 * time.Millisecond,
	}

	testRequest(t, "", req, deleteTopicsRequestV4)
}

func TestDeleteTopicsRequestV5(t *testing.T) {
	req := &DeleteTopicsRequest{
		Version: 5,
		Topics:  []string{"topic", "other"},
		Timeout: 100 * time.Millisecond,
	}

	testRequest(t, "", req, deleteTopicsRequestV5)
}

func TestDeleteTopicsRequestV6(t *testing.T) {
	req := &DeleteTopicsRequest{
		Version: 6,
		Topics:  []string{"topic", "other"},
		Timeout: 100 * time.Millisecond,
	}

	testRequest(t, "", req, deleteTopicsRequestV6)
}

//go:build !functional

package sarama

import (
	"bytes"
	"testing"
)

var (
	electLeadersRequestOneTopicV1 = []byte{
		0,          // preferred election type
		0, 0, 0, 1, // 1 topic
		0, 5, 116, 111, 112, 105, 99, // topic name "topic" as compact string
		0, 0, 0, 1, // 1 partition
		0, 0, 0, 0, // partition 0
		0, 0, 39, 16, // timeout 10000
	}
	electLeadersRequestOneTopicV2 = []byte{
		0,                         // preferred election type
		2,                         // 2-1=1 topic
		6, 116, 111, 112, 105, 99, // topic name "topic" as compact string
		2,          // 2-1=1 partition
		0, 0, 0, 0, // partition 0
		0,            // empty tagged fields
		0, 0, 39, 16, // timeout 10000
		0, // empty tagged fields
	}
)

func TestElectLeadersRequest(t *testing.T) {
	var request = &ElectLeadersRequest{
		TimeoutMs: int32(10000),
		Version:   int16(1),
		TopicPartitions: map[string][]int32{
			"topic": {0},
		},
		Type: PreferredElection,
	}

	testRequest(t, "one topic V1", request, electLeadersRequestOneTopicV1)

	request.Version = 2
	testRequest(t, "one topic V2", request, electLeadersRequestOneTopicV2)
}

func TestElectLeadersRequestHeaderVersion(t *testing.T) {
	reqBody := &ElectLeadersRequest{
		TimeoutMs: int32(10000),
		Version:   int16(1),
		TopicPartitions: map[string][]int32{
			"topic": {0},
		},
		Type: PreferredElection,
	}

	packet, err := encode(&request{
		correlationID: 123,
		clientID:      "foo",
		body:          reqBody,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	nonFlexibleHeaderSize := 14 + len("foo")
	if !bytes.Equal(packet[nonFlexibleHeaderSize:], electLeadersRequestOneTopicV1) {
		t.Fatalf("expected V1 body to start immediately after request header")
	}

	reqBody.Version = 2
	packet, err = encode(&request{
		correlationID: 123,
		clientID:      "foo",
		body:          reqBody,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	flexibleHeaderSize := nonFlexibleHeaderSize + 1
	if packet[nonFlexibleHeaderSize] != 0 {
		t.Fatalf("expected V2 flexible request header to include empty tagged fields, got byte %d", packet[nonFlexibleHeaderSize])
	}
	if !bytes.Equal(packet[flexibleHeaderSize:], electLeadersRequestOneTopicV2) {
		t.Fatalf("expected V2 body to start after flexible request header")
	}
}

func TestElectLeadersResponseHeaderVersion(t *testing.T) {
	response := &ElectLeadersResponse{Version: 1}
	if response.headerVersion() != 0 {
		t.Fatalf("expected V1 response header version 0, got %d", response.headerVersion())
	}

	response.Version = 2
	if response.headerVersion() != 1 {
		t.Fatalf("expected V2 response header version 1, got %d", response.headerVersion())
	}
}

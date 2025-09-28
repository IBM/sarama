//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var (
	createTopicsRequestV0 = []byte{
		0, 0, 0, 1,
		0, 5, 't', 'o', 'p', 'i', 'c',
		255, 255, 255, 255,
		255, 255,
		0, 0, 0, 1, // 1 replica assignment
		0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2,
		0, 0, 0, 1, // 1 config
		0, 12, 'r', 'e', 't', 'e', 'n', 't', 'i', 'o', 'n', '.', 'm', 's',
		0, 2, '-', '1',
		0, 0, 0, 100,
	}

	createTopicsRequestV1 = append(createTopicsRequestV0, byte(1))

	createTopicsRequestV5 = []byte{
		2,
		6, 't', 'o', 'p', 'i', 'c',
		255, 255, 255, 255,
		255, 255,
		2,          // 1 replica assignment
		0, 0, 0, 0, // partition index
		4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, // broker ids
		0, // empty tagged fields
		2, // 1 config
		13, 'r', 'e', 't', 'e', 'n', 't', 'i', 'o', 'n', '.', 'm', 's',
		3, '-', '1',
		0, // empty tagged fields
		0, // empty tagged fields
		0, 0, 0, 100,
		1,
		0, // empty tagged fields
	}
)

func TestCreateTopicsRequest(t *testing.T) {
	retention := "-1"

	req := &CreateTopicsRequest{
		TopicDetails: map[string]*TopicDetail{
			"topic": {
				NumPartitions:     -1,
				ReplicationFactor: -1,
				ReplicaAssignment: map[int32][]int32{
					0: {0, 1, 2},
				},
				ConfigEntries: map[string]*string{
					"retention.ms": &retention,
				},
			},
		},
		Timeout: 100 * time.Millisecond,
	}

	testRequest(t, "version 0", req, createTopicsRequestV0)

	req.Version = 1
	req.ValidateOnly = true

	testRequest(t, "version 1", req, createTopicsRequestV1)

	req.Version = 5
	testRequest(t, "version 5", req, createTopicsRequestV5)
}

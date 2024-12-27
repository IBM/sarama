//go:build !functional

package sarama

import (
	"testing"
)

var (
	emptyDeleteOffsetsResponse = []byte{
		0, 0, // no error
		0, 0, 0, 0, // 0 throttle
		0, 0, 0, 0, // 0 topics
	}

	errorDeleteOffsetsResponse = []byte{
		0, 16, // error 16 : ErrNotCoordinatorForConsumer
		0, 0, 0, 0, // 0 throttle
		0, 0, 0, 1, // 1 topic
		0, 3, 'b', 'a', 'r', // topic name: bar
		0, 0, 0, 1, // 1 partition
		0, 0, 0, 6, // partition 6
		0, 0, // no error
	}

	errorOnPartitionResponse = []byte{
		0, 0, // no error
		0, 0, 0, 0, // 0 throttle
		0, 0, 0, 1, // 1 topic
		0, 3, 'b', 'a', 'r', // topic name: bar
		0, 0, 0, 1, // 1 partition
		0, 0, 0, 6, // partition 6
		0, 86, // error ErrGroupSubscribedToTopic=86
	}
)

func TestDeleteOffsetsResponse(t *testing.T) {
	var response *DeleteOffsetsResponse

	response = &DeleteOffsetsResponse{
		ErrorCode:    0,
		ThrottleTime: 0,
	}
	testResponse(t, "empty no error", response, emptyDeleteOffsetsResponse)

	response = &DeleteOffsetsResponse{
		ErrorCode:    0,
		ThrottleTime: 0,
		Errors: map[string]map[int32]KError{
			"bar": {
				6: 0,
				7: 0,
			},
		},
	}
	// The response encoded form cannot be checked for it varies due to
	// unpredictable map traversal order.
	testResponse(t, "no error", response, nil)

	response = &DeleteOffsetsResponse{
		ErrorCode:    16,
		ThrottleTime: 0,
		Errors: map[string]map[int32]KError{
			"bar": {
				6: 0,
			},
		},
	}
	testResponse(t, "error global", response, errorDeleteOffsetsResponse)

	response = &DeleteOffsetsResponse{
		ErrorCode:    0,
		ThrottleTime: 0,
	}
	response.AddError("bar", 6, ErrGroupSubscribedToTopic)
	testResponse(t, "error partition", response, errorOnPartitionResponse)
}

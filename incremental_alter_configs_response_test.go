//go:build !functional

package sarama

import (
	"testing"
)

var (
	incrementalAlterResponseEmpty = []byte{
		0, 0, 0, 0, // throttle
		0, 0, 0, 0, // no configs
	}

	incrementalAlterResponsePopulated = []byte{
		0, 0, 0, 0, // throttle
		0, 0, 0, 1, // response
		0, 0, // errorcode
		0, 0, // string
		2, // topic
		0, 3, 'f', 'o', 'o',
	}

	incrementalAlterResponseEmptyV1 = []byte{
		0, 0, 0, 0, // throttle
		1, // no configs
		0, // empty tagged fields
	}

	incrementalAlterResponsePopulatedV1 = []byte{
		0, 0, 0, 0, // throttle
		2,    // response
		0, 0, // errorcode
		1, // empty string
		2, // topic
		4, 'f', 'o', 'o',
		0, // empty tagged fields
		0, // empty tagged fields
	}
)

func TestIncrementalAlterConfigsResponse(t *testing.T) {
	var response *IncrementalAlterConfigsResponse

	response = &IncrementalAlterConfigsResponse{
		Resources: []*AlterConfigsResourceResponse{},
	}
	testVersionDecodable(t, "empty", response, incrementalAlterResponseEmpty, 0)
	if len(response.Resources) != 0 {
		t.Error("Expected no groups")
	}

	response = &IncrementalAlterConfigsResponse{
		Resources: []*AlterConfigsResourceResponse{
			{
				ErrorCode: 0,
				ErrorMsg:  "",
				Type:      TopicResource,
				Name:      "foo",
			},
		},
	}
	testResponse(t, "response with error", response, incrementalAlterResponsePopulated)
}

func TestIncrementalAlterConfigsResponseV1(t *testing.T) {
	var response *IncrementalAlterConfigsResponse

	response = &IncrementalAlterConfigsResponse{
		Resources: []*AlterConfigsResourceResponse{},
	}
	testVersionDecodable(t, "empty", response, incrementalAlterResponseEmptyV1, 1)
	if len(response.Resources) != 0 {
		t.Error("Expected no groups")
	}

	response = &IncrementalAlterConfigsResponse{
		Resources: []*AlterConfigsResourceResponse{
			{
				ErrorCode: 0,
				ErrorMsg:  "",
				Type:      TopicResource,
				Name:      "foo",
			},
		},
	}
	testVersionDecodable(t, "response with error", response, incrementalAlterResponsePopulatedV1, 1)
}

//go:build !functional

package sarama

import "testing"

var (
	emptyIncrementalAlterConfigsRequest = []byte{
		0, 0, 0, 0, // 0 configs
		0, // don't Validate
	}

	singleIncrementalAlterConfigsRequest = []byte{
		0, 0, 0, 1, // 1 config
		2,                   // a topic
		0, 3, 'f', 'o', 'o', // topic name: foo
		0, 0, 0, 1, // 1 config name
		0, 10, // 10 chars
		's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		0, // OperationSet
		0, 4,
		'1', '0', '0', '0',
		0, // don't validate
	}

	doubleIncrementalAlterConfigsRequest = []byte{
		0, 0, 0, 2, // 2 config
		2,                   // a topic
		0, 3, 'f', 'o', 'o', // topic name: foo
		0, 0, 0, 1, // 1 config name
		0, 10, // 10 chars
		's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		0, // OperationSet
		0, 4,
		'1', '0', '0', '0',
		2,                   // a topic
		0, 3, 'b', 'a', 'r', // topic name: foo
		0, 0, 0, 1, // 2 config
		0, 12, // 12 chars
		'r', 'e', 't', 'e', 'n', 't', 'i', 'o', 'n', '.', 'm', 's',
		1, // OperationDelete
		0, 4,
		'1', '0', '0', '0',
		0, // don't validate
	}

	emptyIncrementalAlterConfigsRequestV1 = []byte{
		1, // 0 configs
		0, // don't Validate
		0, // empty tagged fields
	}

	singleIncrementalAlterConfigsRequestV1 = []byte{
		2,                // 1 config
		2,                // a topic
		4, 'f', 'o', 'o', // topic name: foo
		2,  // 1 config name
		11, // 10 chars
		's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		0, // OperationSet
		5,
		'1', '0', '0', '0',
		0, // empty tagged fields
		0, // empty tagged fields
		0, // don't validate
		0, // empty tagged fields
	}

	doubleIncrementalAlterConfigsRequestV1 = []byte{
		3,                // 2 config
		2,                // a topic
		4, 'f', 'o', 'o', // topic name: foo
		2,  // 1 config name
		11, // 10 chars
		's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		0, // OperationSet
		5,
		'1', '0', '0', '0',
		0,                // empty tagged fields
		0,                // empty tagged fields
		2,                // a topic
		4, 'b', 'a', 'r', // topic name: foo
		2,  // 2 config
		13, // 12 chars
		'r', 'e', 't', 'e', 'n', 't', 'i', 'o', 'n', '.', 'm', 's',
		1, // OperationDelete
		5,
		'1', '0', '0', '0',
		0, // empty tagged fields
		0, // empty tagged fields
		0, // don't validate
		0, // empty tagged fields
	}
)

func TestIncrementalAlterConfigsRequest(t *testing.T) {
	var request *IncrementalAlterConfigsRequest

	request = &IncrementalAlterConfigsRequest{
		Resources: []*IncrementalAlterConfigsResource{},
	}
	testRequest(t, "no requests", request, emptyIncrementalAlterConfigsRequest)

	configValue := "1000"
	request = &IncrementalAlterConfigsRequest{
		Resources: []*IncrementalAlterConfigsResource{
			{
				Type: TopicResource,
				Name: "foo",
				ConfigEntries: map[string]IncrementalAlterConfigsEntry{
					"segment.ms": {
						Operation: IncrementalAlterConfigsOperationSet,
						Value:     &configValue,
					},
				},
			},
		},
	}

	testRequest(t, "one config", request, singleIncrementalAlterConfigsRequest)

	request = &IncrementalAlterConfigsRequest{
		Resources: []*IncrementalAlterConfigsResource{
			{
				Type: TopicResource,
				Name: "foo",
				ConfigEntries: map[string]IncrementalAlterConfigsEntry{
					"segment.ms": {
						Operation: IncrementalAlterConfigsOperationSet,
						Value:     &configValue,
					},
				},
			},
			{
				Type: TopicResource,
				Name: "bar",
				ConfigEntries: map[string]IncrementalAlterConfigsEntry{
					"retention.ms": {
						Operation: IncrementalAlterConfigsOperationDelete,
						Value:     &configValue,
					},
				},
			},
		},
	}

	testRequest(t, "two configs", request, doubleIncrementalAlterConfigsRequest)
}

func TestIncrementalAlterConfigsRequestV1(t *testing.T) {
	var request *IncrementalAlterConfigsRequest

	request = &IncrementalAlterConfigsRequest{
		Version:   1,
		Resources: []*IncrementalAlterConfigsResource{},
	}
	testRequest(t, "no requests", request, emptyIncrementalAlterConfigsRequestV1)

	configValue := "1000"
	request = &IncrementalAlterConfigsRequest{
		Version: 1,
		Resources: []*IncrementalAlterConfigsResource{
			{
				Type: TopicResource,
				Name: "foo",
				ConfigEntries: map[string]IncrementalAlterConfigsEntry{
					"segment.ms": {
						Operation: IncrementalAlterConfigsOperationSet,
						Value:     &configValue,
					},
				},
			},
		},
	}

	testRequest(t, "one config", request, singleIncrementalAlterConfigsRequestV1)

	request = &IncrementalAlterConfigsRequest{
		Version: 1,
		Resources: []*IncrementalAlterConfigsResource{
			{
				Type: TopicResource,
				Name: "foo",
				ConfigEntries: map[string]IncrementalAlterConfigsEntry{
					"segment.ms": {
						Operation: IncrementalAlterConfigsOperationSet,
						Value:     &configValue,
					},
				},
			},
			{
				Type: TopicResource,
				Name: "bar",
				ConfigEntries: map[string]IncrementalAlterConfigsEntry{
					"retention.ms": {
						Operation: IncrementalAlterConfigsOperationDelete,
						Value:     &configValue,
					},
				},
			},
		},
	}

	testRequest(t, "two configs", request, doubleIncrementalAlterConfigsRequestV1)
}

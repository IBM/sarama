package sarama

import "testing"

var (
	emptyAlterConfigsRequest = []byte{
		0, 0, 0, 0, // 0 configs
		0, // don't Validate
	}

	singleAlterConfigsRequest = []byte{
		0, 0, 0, 1, // 1 config
		2,                   // a topic
		0, 3, 'f', 'o', 'o', // topic name: foo
		0, 0, 0, 1, //1 config name
		0, 10, // 10 chars
		's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		0, 4,
		'1', '0', '0', '0',
		0, // don't validate
	}

	doubleAlterConfigsRequest = []byte{
		0, 0, 0, 2, // 2 config
		2,                   // a topic
		0, 3, 'f', 'o', 'o', // topic name: foo
		0, 0, 0, 1, //1 config name
		0, 10, // 10 chars
		's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		0, 4,
		'1', '0', '0', '0',
		2,                   // a topic
		0, 3, 'b', 'a', 'r', // topic name: foo
		0, 0, 0, 2, //2 config
		0, 10, // 10 chars
		's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		0, 4,
		'1', '0', '0', '0',
		0, 12, // 12 chars
		'r', 'e', 't', 'e', 'n', 't', 'i', 'o', 'n', '.', 'm', 's',
		0, 4,
		'1', '0', '0', '0',
		0, // don't validate
	}
)

func TestAlterConfigsRequest(t *testing.T) {
	var request *AlterConfigsRequest

	request = &AlterConfigsRequest{
		Resources: []*AlterConfigsResource{},
	}
	testRequest(t, "no requests", request, emptyAlterConfigsRequest)

	request = &AlterConfigsRequest{
		Resources: []*AlterConfigsResource{
			&AlterConfigsResource{
				T:    TopicResource,
				Name: "foo",
				ConfigEntries: []*ConfigEntryKV{
					&ConfigEntryKV{
						Name:  "segment.ms",
						Value: "1000",
					},
				},
			},
		},
	}

	testRequest(t, "one config", request, singleAlterConfigsRequest)

	request = &AlterConfigsRequest{
		Resources: []*AlterConfigsResource{
			&AlterConfigsResource{
				T:    TopicResource,
				Name: "foo",
				ConfigEntries: []*ConfigEntryKV{
					&ConfigEntryKV{
						Name:  "segment.ms",
						Value: "1000",
					},
				},
			},
			&AlterConfigsResource{
				T:    TopicResource,
				Name: "bar",
				ConfigEntries: []*ConfigEntryKV{
					&ConfigEntryKV{
						Name:  "segment.ms",
						Value: "1000",
					},
					&ConfigEntryKV{
						Name:  "retention.ms",
						Value: "1000",
					},
				},
			},
		},
	}

	testRequest(t, "two configs", request, doubleAlterConfigsRequest)
}

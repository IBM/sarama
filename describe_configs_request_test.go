package sarama

import "testing"

var (
	emptyDescribeConfigsRequest = []byte{
		0, 0, 0, 0, // 0 configs
	}

	singleDescribeConfigsRequest = []byte{
		0, 0, 0, 1, // 1 config
		2,                   // a topic
		0, 3, 'f', 'o', 'o', // topic name: foo
		0, 0, 0, 1, //1 config name
		0, 10, // 10 chars
		's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
	}

	doubleDescribeConfigsRequest = []byte{
		0, 0, 0, 2, // 2 configs
		2,                   // a topic
		0, 3, 'f', 'o', 'o', // topic name: foo
		0, 0, 0, 2, //2 config name
		0, 10, // 10 chars
		's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		0, 12, // 12 chars
		'r', 'e', 't', 'e', 'n', 't', 'i', 'o', 'n', '.', 'm', 's',
		2,                   // a topic
		0, 3, 'b', 'a', 'r', // topic name: foo
		0, 0, 0, 1, // 1 config
		0, 10, // 10 chars
		's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
	}
)

func TestDescribeConfigsRequest(t *testing.T) {
	var request *DescribeConfigsRequest

	request = &DescribeConfigsRequest{
		Resources: []*Resource{},
	}
	testRequest(t, "no requests", request, emptyDescribeConfigsRequest)

	request = &DescribeConfigsRequest{
		Resources: []*Resource{
			&Resource{
				Type:        TopicResource,
				Name:        "foo",
				ConfigNames: []string{"segment.ms"},
			},
		},
	}

	testRequest(t, "one config", request, singleDescribeConfigsRequest)

	request = &DescribeConfigsRequest{
		Resources: []*Resource{
			&Resource{
				Type:        TopicResource,
				Name:        "foo",
				ConfigNames: []string{"segment.ms", "retention.ms"},
			},
			&Resource{
				Type:        TopicResource,
				Name:        "bar",
				ConfigNames: []string{"segment.ms"},
			},
		},
	}
	testRequest(t, "two configs", request, doubleDescribeConfigsRequest)
}

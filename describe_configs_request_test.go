//go:build !functional

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
		0, 0, 0, 1, // 1 config name
		0, 10, // 10 chars
		's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
	}

	doubleDescribeConfigsRequest = []byte{
		0, 0, 0, 2, // 2 configs
		2,                   // a topic
		0, 3, 'f', 'o', 'o', // topic name: foo
		0, 0, 0, 2, // 2 config name
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

	singleDescribeConfigsRequestAllConfigs = []byte{
		0, 0, 0, 1, // 1 config
		2,                   // a topic
		0, 3, 'f', 'o', 'o', // topic name: foo
		255, 255, 255, 255, // all configs
	}

	singleDescribeConfigsRequestAllConfigsv1 = []byte{
		0, 0, 0, 1, // 1 config
		2,                   // a topic
		0, 3, 'f', 'o', 'o', // topic name: foo
		255, 255, 255, 255, // no configs
		1, // synonyms
	}

	singleDescribeConfigsRequestv3 = []byte{
		0, 0, 0, 1, // 1 config
		2,                   // a topic
		0, 3, 'f', 'o', 'o', // topic name: foo
		0, 0, 0, 1, // 1 config name
		0, 10, // 10 chars
		's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		1, // synonyms
		1, // documentation
	}

	singleDescribeConfigsRequestv4 = []byte{
		2,                // 1 config (compact array)
		2,                // a topic
		4, 'f', 'o', 'o', // topic name: foo (compact string)
		2,  // 1 config name (compact array)
		11, // 10 chars (compact string)
		's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		0, // resource tagged fields
		1, // synonyms
		1, // documentation
		0, // request tagged fields
	}
)

func TestDescribeConfigsRequestv0(t *testing.T) {
	var request *DescribeConfigsRequest

	request = &DescribeConfigsRequest{
		Version:   0,
		Resources: []*ConfigResource{},
	}
	testRequest(t, "no requests", request, emptyDescribeConfigsRequest)

	configs := []string{"segment.ms"}
	request = &DescribeConfigsRequest{
		Version: 0,
		Resources: []*ConfigResource{
			{
				Type:        TopicResource,
				Name:        "foo",
				ConfigNames: configs,
			},
		},
	}

	testRequest(t, "one config", request, singleDescribeConfigsRequest)

	request = &DescribeConfigsRequest{
		Version: 0,
		Resources: []*ConfigResource{
			{
				Type:        TopicResource,
				Name:        "foo",
				ConfigNames: []string{"segment.ms", "retention.ms"},
			},
			{
				Type:        TopicResource,
				Name:        "bar",
				ConfigNames: []string{"segment.ms"},
			},
		},
	}
	testRequest(t, "two configs", request, doubleDescribeConfigsRequest)

	request = &DescribeConfigsRequest{
		Version: 0,
		Resources: []*ConfigResource{
			{
				Type: TopicResource,
				Name: "foo",
			},
		},
	}

	testRequest(t, "one topic, all configs", request, singleDescribeConfigsRequestAllConfigs)
}

func TestDescribeConfigsRequestv1(t *testing.T) {
	request := &DescribeConfigsRequest{
		Version: 1,
		Resources: []*ConfigResource{
			{
				Type: TopicResource,
				Name: "foo",
			},
		},
		IncludeSynonyms: true,
	}

	testRequest(t, "one topic, all configs", request, singleDescribeConfigsRequestAllConfigsv1)
}

func TestDescribeConfigsRequestv3(t *testing.T) {
	request := &DescribeConfigsRequest{
		Version: 3,
		Resources: []*ConfigResource{
			{
				Type:        TopicResource,
				Name:        "foo",
				ConfigNames: []string{"segment.ms"},
			},
		},
		IncludeSynonyms:      true,
		IncludeDocumentation: true,
	}

	testRequest(t, "include synonyms and documentation", request, singleDescribeConfigsRequestv3)
}

func TestDescribeConfigsRequestv4(t *testing.T) {
	request := &DescribeConfigsRequest{
		Version: 4,
		Resources: []*ConfigResource{
			{
				Type:        TopicResource,
				Name:        "foo",
				ConfigNames: []string{"segment.ms"},
			},
		},
		IncludeSynonyms:      true,
		IncludeDocumentation: true,
	}

	testRequest(t, "include synonyms and documentation", request, singleDescribeConfigsRequestv4)
}

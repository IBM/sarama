//go:build !functional

package sarama

import (
	"testing"
)

var (
	describeConfigsResponseEmpty = []byte{
		0, 0, 0, 0, // throttle
		0, 0, 0, 0, // no configs
	}

	describeConfigsResponsePopulatedv0 = []byte{
		0, 0, 0, 0, // throttle
		0, 0, 0, 1, // response
		0, 0, // errorcode
		0, 0, // string
		2, // topic
		0, 3, 'f', 'o', 'o',
		0, 0, 0, 1, // configs
		0, 10, 's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		0, 4, '1', '0', '0', '0',
		0, // ReadOnly
		0, // Default
		0, // Sensitive
	}

	describeConfigsResponseWithDefaultv0 = []byte{
		0, 0, 0, 0, // throttle
		0, 0, 0, 1, // response
		0, 0, // errorcode
		0, 0, // string
		2, // topic
		0, 3, 'f', 'o', 'o',
		0, 0, 0, 1, // configs
		0, 10, 's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		0, 4, '1', '0', '0', '0',
		0, // ReadOnly
		1, // Default
		0, // Sensitive
	}

	describeConfigsResponsePopulatedv1 = []byte{
		0, 0, 0, 0, // throttle
		0, 0, 0, 1, // response
		0, 0, // errorcode
		0, 0, // string
		2, // topic
		0, 3, 'f', 'o', 'o',
		0, 0, 0, 1, // configs
		0, 10, 's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		0, 4, '1', '0', '0', '0',
		0,          // ReadOnly
		4,          // Source
		0,          // Sensitive
		0, 0, 0, 0, // No Synonym
	}

	describeConfigsResponseWithSynonymv1 = []byte{
		0, 0, 0, 0, // throttle
		0, 0, 0, 1, // response
		0, 0, // errorcode
		0, 0, // string
		2, // topic
		0, 3, 'f', 'o', 'o',
		0, 0, 0, 1, // configs
		0, 10, 's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		0, 4, '1', '0', '0', '0',
		0,          // ReadOnly
		4,          // Source
		0,          // Sensitive
		0, 0, 0, 1, // 1 Synonym
		0, 14, 'l', 'o', 'g', '.', 's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		0, 4, '1', '0', '0', '0',
		4, // Source
	}

	describeConfigsResponseWithDefaultv1 = []byte{
		0, 0, 0, 0, // throttle
		0, 0, 0, 1, // response
		0, 0, // errorcode
		0, 0, // string
		2, // topic
		0, 3, 'f', 'o', 'o',
		0, 0, 0, 1, // configs
		0, 10, 's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		0, 4, '1', '0', '0', '0',
		0,          // ReadOnly
		5,          // Source
		0,          // Sensitive
		0, 0, 0, 0, // No Synonym
	}
)

func TestDescribeConfigsResponsev0(t *testing.T) {
	var response *DescribeConfigsResponse

	response = &DescribeConfigsResponse{
		Resources: []*ResourceResponse{},
	}
	testVersionDecodable(t, "empty", response, describeConfigsResponseEmpty, 0)
	if len(response.Resources) != 0 {
		t.Error("Expected no groups")
	}

	response = &DescribeConfigsResponse{
		Version: 0, Resources: []*ResourceResponse{
			{
				ErrorCode: 0,
				ErrorMsg:  "",
				Type:      TopicResource,
				Name:      "foo",
				Configs: []*ConfigEntry{
					{
						Name:      "segment.ms",
						Value:     "1000",
						ReadOnly:  false,
						Default:   false,
						Sensitive: false,
						Source:    SourceUnknown,
					},
				},
			},
		},
	}
	testResponse(t, "response with error", response, describeConfigsResponsePopulatedv0)
}

func TestDescribeConfigsResponseWithDefaultv0(t *testing.T) {
	var response *DescribeConfigsResponse

	response = &DescribeConfigsResponse{
		Resources: []*ResourceResponse{},
	}
	testVersionDecodable(t, "empty", response, describeConfigsResponseEmpty, 0)
	if len(response.Resources) != 0 {
		t.Error("Expected no groups")
	}

	response = &DescribeConfigsResponse{
		Version: 0, Resources: []*ResourceResponse{
			{
				ErrorCode: 0,
				ErrorMsg:  "",
				Type:      TopicResource,
				Name:      "foo",
				Configs: []*ConfigEntry{
					{
						Name:      "segment.ms",
						Value:     "1000",
						ReadOnly:  false,
						Default:   true,
						Sensitive: false,
						Source:    SourceDefault,
					},
				},
			},
		},
	}
	testResponse(t, "response with default", response, describeConfigsResponseWithDefaultv0)
}

func TestDescribeConfigsResponsev1(t *testing.T) {
	var response *DescribeConfigsResponse

	response = &DescribeConfigsResponse{
		Resources: []*ResourceResponse{},
	}
	testVersionDecodable(t, "empty", response, describeConfigsResponseEmpty, 0)
	if len(response.Resources) != 0 {
		t.Error("Expected no groups")
	}

	response = &DescribeConfigsResponse{
		Version: 1,
		Resources: []*ResourceResponse{
			{
				ErrorCode: 0,
				ErrorMsg:  "",
				Type:      TopicResource,
				Name:      "foo",
				Configs: []*ConfigEntry{
					{
						Name:      "segment.ms",
						Value:     "1000",
						ReadOnly:  false,
						Source:    SourceStaticBroker,
						Default:   false,
						Sensitive: false,
						Synonyms:  []*ConfigSynonym{},
					},
				},
			},
		},
	}
	testResponse(t, "response with error", response, describeConfigsResponsePopulatedv1)
}

func TestDescribeConfigsResponseWithSynonym(t *testing.T) {
	var response *DescribeConfigsResponse

	response = &DescribeConfigsResponse{
		Resources: []*ResourceResponse{},
	}
	testVersionDecodable(t, "empty", response, describeConfigsResponseEmpty, 0)
	if len(response.Resources) != 0 {
		t.Error("Expected no groups")
	}

	response = &DescribeConfigsResponse{
		Version: 1,
		Resources: []*ResourceResponse{
			{
				ErrorCode: 0,
				ErrorMsg:  "",
				Type:      TopicResource,
				Name:      "foo",
				Configs: []*ConfigEntry{
					{
						Name:      "segment.ms",
						Value:     "1000",
						ReadOnly:  false,
						Source:    SourceStaticBroker,
						Default:   false,
						Sensitive: false,
						Synonyms: []*ConfigSynonym{
							{
								ConfigName:  "log.segment.ms",
								ConfigValue: "1000",
								Source:      SourceStaticBroker,
							},
						},
					},
				},
			},
		},
	}
	testResponse(t, "response with error", response, describeConfigsResponseWithSynonymv1)
}

func TestDescribeConfigsResponseWithDefaultv1(t *testing.T) {
	var response *DescribeConfigsResponse

	response = &DescribeConfigsResponse{
		Resources: []*ResourceResponse{},
	}
	testVersionDecodable(t, "empty", response, describeConfigsResponseEmpty, 0)
	if len(response.Resources) != 0 {
		t.Error("Expected no groups")
	}

	response = &DescribeConfigsResponse{
		Version: 1,
		Resources: []*ResourceResponse{
			{
				ErrorCode: 0,
				ErrorMsg:  "",
				Type:      TopicResource,
				Name:      "foo",
				Configs: []*ConfigEntry{
					{
						Name:      "segment.ms",
						Value:     "1000",
						ReadOnly:  false,
						Source:    SourceDefault,
						Default:   true,
						Sensitive: false,
						Synonyms:  []*ConfigSynonym{},
					},
				},
			},
		},
	}
	testResponse(t, "response with error", response, describeConfigsResponseWithDefaultv1)
}

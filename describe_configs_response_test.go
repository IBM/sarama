//go:build !functional

package sarama

import (
	"errors"
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

	describeConfigsResponseWithDocumentationv3 = []byte{
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
		4,                                            // Source
		3,                                            // ConfigType: INT
		0, 8, 't', 'h', 'e', ' ', 'd', 'o', 'c', 's', // Documentation
	}

	describeConfigsResponseWithDocumentationv4 = []byte{
		0, 0, 0, 0, // throttle
		2,    // 1 response (compact array)
		0, 0, // errorcode
		1,                // empty error message (compact string)
		2,                // topic
		4, 'f', 'o', 'o', // resource name (compact string)
		2,  // 1 config (compact array)
		11, // segment.ms (compact string)
		's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's',
		5, '1', '0', '0', '0', // value (compact string)
		0,                                                                        // ReadOnly
		4,                                                                        // Source
		0,                                                                        // Sensitive
		2,                                                                        // 1 Synonym (compact array)
		15, 'l', 'o', 'g', '.', 's', 'e', 'g', 'm', 'e', 'n', 't', '.', 'm', 's', // synonym name (compact string)
		5, '1', '0', '0', '0', // synonym value (compact string)
		4,                                         // Source
		0,                                         // synonym tagged fields
		3,                                         // ConfigType: INT
		9, 't', 'h', 'e', ' ', 'd', 'o', 'c', 's', // Documentation (compact string)
		0, // config tagged fields
		0, // resource tagged fields
		0, // response tagged fields
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

func TestDescribeConfigsResponseWithDocumentationv3(t *testing.T) {
	var response *DescribeConfigsResponse

	response = &DescribeConfigsResponse{
		Resources: []*ResourceResponse{},
	}
	testVersionDecodable(t, "empty", response, describeConfigsResponseEmpty, 3)
	if len(response.Resources) != 0 {
		t.Error("Expected no groups")
	}

	response = &DescribeConfigsResponse{
		Version: 3,
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
						Type:          IntConfigType,
						Documentation: nullString("the docs"),
					},
				},
			},
		},
	}
	testResponse(t, "response with documentation", response, describeConfigsResponseWithDocumentationv3)
}

func TestDescribeConfigsResponseWithDocumentationv4(t *testing.T) {
	response := &DescribeConfigsResponse{
		Version: 4,
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
						Type:          IntConfigType,
						Documentation: nullString("the docs"),
					},
				},
			},
		},
	}
	testResponse(t, "response with documentation", response, describeConfigsResponseWithDocumentationv4)
}

func TestDescribeConfigError(t *testing.T) {
	// Assert that DescribeConfigError satisfies error interface
	var err error = &DescribeConfigError{
		Err: ErrInvalidConfig,
	}

	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected errors.Is to match ErrInvalidConfig")
	}

	got := err.Error()
	want := ErrInvalidConfig.Error()
	if got != want {
		t.Errorf("DescribeConfigError.Error() = %v; want %v", got, want)
	}

	err = &DescribeConfigError{
		Err:    ErrInvalidConfig,
		ErrMsg: "invalid config value",
	}
	got = err.Error()
	want = ErrInvalidConfig.Error() + " - invalid config value"
	if got != want {
		t.Errorf("DescribeConfigError.Error() = %v; want %v", got, want)
	}

	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected errors.Is to match ErrInvalidConfig with ErrMsg set")
	}
}

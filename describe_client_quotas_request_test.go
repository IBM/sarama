//go:build !functional

package sarama

import "testing"

var (
	describeClientQuotasRequestAll = []byte{
		0, 0, 0, 0, // components len
		0, // strict
	}

	describeClientQuotasRequestDefaultUser = []byte{
		0, 0, 0, 1, // components len
		0, 4, 'u', 's', 'e', 'r', // entity type
		1,    // match type (default)
		0, 0, // match *string
		0, // strict
	}

	describeClientQuotasRequestOnlySpecificUser = []byte{
		0, 0, 0, 1, // components len
		0, 4, 'u', 's', 'e', 'r', // entity type
		0,                                  // match type (exact)
		0, 6, 's', 'a', 'r', 'a', 'm', 'a', // match *string
		1, // strict
	}

	describeClientQuotasRequestMultiComponents = []byte{
		0, 0, 0, 2, // components len
		0, 4, 'u', 's', 'e', 'r', // entity type
		2,        // match type (any)
		255, 255, // match *string
		0, 9, 'c', 'l', 'i', 'e', 'n', 't', '-', 'i', 'd', // entity type
		1,    // match type (default)
		0, 0, // match *string
		0, // strict
	}
)

func TestDescribeClientQuotasRequest(t *testing.T) {
	// Match All
	req := &DescribeClientQuotasRequest{
		Components: []QuotaFilterComponent{},
		Strict:     false,
	}
	testRequest(t, "Match All", req, describeClientQuotasRequestAll)

	// Match Default User
	defaultUser := QuotaFilterComponent{
		EntityType: QuotaEntityUser,
		MatchType:  QuotaMatchDefault,
	}
	req = &DescribeClientQuotasRequest{
		Components: []QuotaFilterComponent{defaultUser},
		Strict:     false,
	}
	testRequest(t, "Match Default User", req, describeClientQuotasRequestDefaultUser)

	// Match Only Specific User
	specificUser := QuotaFilterComponent{
		EntityType: QuotaEntityUser,
		MatchType:  QuotaMatchExact,
		Match:      "sarama",
	}
	req = &DescribeClientQuotasRequest{
		Components: []QuotaFilterComponent{specificUser},
		Strict:     true,
	}
	testRequest(t, "Match Only Specific User", req, describeClientQuotasRequestOnlySpecificUser)

	// Match default client-id of any user
	anyUser := QuotaFilterComponent{
		EntityType: QuotaEntityUser,
		MatchType:  QuotaMatchAny,
	}
	defaultClientId := QuotaFilterComponent{
		EntityType: QuotaEntityClientID,
		MatchType:  QuotaMatchDefault,
	}
	req = &DescribeClientQuotasRequest{
		Components: []QuotaFilterComponent{anyUser, defaultClientId},
		Strict:     false,
	}
	testRequest(t, "Match default client-id of any user", req, describeClientQuotasRequestMultiComponents)
}

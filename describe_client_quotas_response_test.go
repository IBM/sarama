//go:build !functional

package sarama

import "testing"

var (
	describeClientQuotasResponseError = []byte{
		0, 0, 0, 0, // ThrottleTime
		0, 35, // ErrorCode
		0, 41, 'C', 'u', 's', 't', 'o', 'm', ' ', 'e', 'n', 't', 'i', 't', 'y', ' ', 't', 'y', 'p', 'e', ' ', '\'', 'f', 'a', 'u', 'l', 't', 'y', '\'', ' ', 'n', 'o', 't', ' ', 's', 'u', 'p', 'p', 'o', 'r', 't', 'e', 'd',
		0, 0, 0, 0, // Entries
	}

	describeClientQuotasResponseSingleValue = []byte{
		0, 0, 0, 0, // ThrottleTime
		0, 0, // ErrorCode
		255, 255, // ErrorMsg (nil)
		0, 0, 0, 1, // Entries
		0, 0, 0, 1, // Entity
		0, 4, 'u', 's', 'e', 'r', // Entity type
		255, 255, // Entity name (nil)
		0, 0, 0, 1, // Values
		0, 18, 'p', 'r', 'o', 'd', 'u', 'c', 'e', 'r', '_', 'b', 'y', 't', 'e', '_', 'r', 'a', 't', 'e',
		65, 46, 132, 128, 0, 0, 0, 0, // 1000000
	}

	describeClientQuotasResponseComplexEntity = []byte{
		0, 0, 0, 0, // ThrottleTime
		0, 0, // ErrorCode
		255, 255, // ErrorMsg (nil)
		0, 0, 0, 2, // Entries
		0, 0, 0, 1, // Entity
		0, 4, 'u', 's', 'e', 'r', // Entity type
		255, 255, // Entity name (nil)
		0, 0, 0, 1, // Values
		0, 18, 'p', 'r', 'o', 'd', 'u', 'c', 'e', 'r', '_', 'b', 'y', 't', 'e', '_', 'r', 'a', 't', 'e',
		65, 46, 132, 128, 0, 0, 0, 0, // 1000000
		0, 0, 0, 1, // Entity
		0, 9, 'c', 'l', 'i', 'e', 'n', 't', '-', 'i', 'd', // Entity type
		0, 6, 's', 'a', 'r', 'a', 'm', 'a', // Entity name
		0, 0, 0, 1, // Values
		0, 18, 'c', 'o', 'n', 's', 'u', 'm', 'e', 'r', '_', 'b', 'y', 't', 'e', '_', 'r', 'a', 't', 'e',
		65, 46, 132, 128, 0, 0, 0, 0, // 1000000
	}
)

func TestDescribeClientQuotasResponse(t *testing.T) {
	// Response With Error
	errMsg := "Custom entity type 'faulty' not supported"
	res := &DescribeClientQuotasResponse{
		ThrottleTime: 0,
		ErrorCode:    ErrUnsupportedVersion,
		ErrorMsg:     &errMsg,
		Entries:      []DescribeClientQuotasEntry{},
	}
	testResponse(t, "Response With Error", res, describeClientQuotasResponseError)

	// Single Quota entry
	defaultUserComponent := QuotaEntityComponent{
		EntityType: QuotaEntityUser,
		MatchType:  QuotaMatchDefault,
	}
	entry := DescribeClientQuotasEntry{
		Entity: []QuotaEntityComponent{defaultUserComponent},
		Values: map[string]float64{"producer_byte_rate": 1000000},
	}
	res = &DescribeClientQuotasResponse{
		ThrottleTime: 0,
		ErrorCode:    ErrNoError,
		ErrorMsg:     nil,
		Entries:      []DescribeClientQuotasEntry{entry},
	}
	testResponse(t, "Single Value", res, describeClientQuotasResponseSingleValue)

	// Complex Quota entry
	saramaClientIDComponent := QuotaEntityComponent{
		EntityType: QuotaEntityClientID,
		MatchType:  QuotaMatchExact,
		Name:       "sarama",
	}
	userEntry := DescribeClientQuotasEntry{
		Entity: []QuotaEntityComponent{defaultUserComponent},
		Values: map[string]float64{"producer_byte_rate": 1000000},
	}
	clientEntry := DescribeClientQuotasEntry{
		Entity: []QuotaEntityComponent{saramaClientIDComponent},
		Values: map[string]float64{"consumer_byte_rate": 1000000},
	}
	res = &DescribeClientQuotasResponse{
		ThrottleTime: 0,
		ErrorCode:    ErrNoError,
		ErrorMsg:     nil,
		Entries:      []DescribeClientQuotasEntry{userEntry, clientEntry},
	}
	testResponse(t, "Complex Quota", res, describeClientQuotasResponseComplexEntity)
}

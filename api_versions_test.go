package sarama

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestrictApiVersion(t *testing.T) {
	// the Kafka version a request is built with comes from conf.Version, the
	// user-set maximum, which a broker's advertised range narrows but never raises
	clamping := []struct {
		name           string
		kafkaVersion   KafkaVersion
		clientVersion  int16
		brokerVersions apiVersionMap
		want           int16
	}{
		{
			name:           "lowers the version to the broker's maximum",
			kafkaVersion:   V2_8_0_0,
			clientVersion:  11,
			brokerVersions: apiVersionMap{apiKeyMetadata: &apiVersionRange{minVersion: 0, maxVersion: 8}},
			want:           8,
		},
		{
			name:           "leaves a version already inside the broker's range",
			kafkaVersion:   V2_4_0_0,
			clientVersion:  9,
			brokerVersions: apiVersionMap{apiKeyMetadata: &apiVersionRange{minVersion: 0, maxVersion: 10}},
			want:           9,
		},
		{
			name:           "holds to the user's maximum below the broker's minimum",
			kafkaVersion:   V0_10_0_0,
			clientVersion:  1,
			brokerVersions: apiVersionMap{apiKeyMetadata: &apiVersionRange{minVersion: 5, maxVersion: 10}},
			want:           1,
		},
		{
			name:           "leaves the version alone when the broker advertised nothing",
			kafkaVersion:   V2_8_0_0,
			clientVersion:  11,
			brokerVersions: apiVersionMap{},
			want:           11,
		},
	}

	for _, tc := range clamping {
		t.Run(tc.name, func(t *testing.T) {
			request := NewMetadataRequest(tc.kafkaVersion, []string{"test-topic"})
			require.Equal(t, tc.clientVersion, request.version())

			require.NoError(t, restrictApiVersion(request, tc.brokerVersions))
			assert.Equal(t, tc.want, request.version())
		})
	}

	t.Run("rejects an API absent from what the broker advertised", func(t *testing.T) {
		request := NewDescribeClusterRequest(V2_8_0_0)

		// a broker older than DescribeCluster advertises the APIs it has and omits this one
		brokerVersions := apiVersionMap{
			apiKeyMetadata: &apiVersionRange{
				minVersion: 0,
				maxVersion: 9,
			},
		}

		err := restrictApiVersion(request, brokerVersions)
		require.ErrorIs(t, err, ErrUnsupportedVersion)
	})
}

//go:build !functional

package sarama

import "testing"

func TestDescribeClusterRequestVersions(t *testing.T) {
	t.Run("v0", func(t *testing.T) {
		req := &DescribeClusterRequest{Version: 0, IncludeClusterAuthorizedOperations: true}
		testRequestWithoutByteComparison(t, "DescribeClusterRequest v0", req)
	})

	t.Run("v1", func(t *testing.T) {
		req := &DescribeClusterRequest{
			Version:      1,
			EndpointType: DescribeClusterEndpointTypeControllers,
		}
		testRequestWithoutByteComparison(t, "DescribeClusterRequest v1", req)
	})

	t.Run("v2", func(t *testing.T) {
		req := &DescribeClusterRequest{
			Version:                            2,
			EndpointType:                       DescribeClusterEndpointTypeBrokers,
			IncludeFencedBrokers:               true,
			IncludeClusterAuthorizedOperations: true,
		}
		testRequestWithoutByteComparison(t, "DescribeClusterRequest v2", req)
	})
}

func TestNewDescribeClusterRequest(t *testing.T) {
	testCases := []struct {
		version     KafkaVersion
		expectedVer int16
	}{
		{V2_8_0_0, 0},
		{V3_7_0_0, 1},
		{V4_0_0_0, 2},
	}

	for _, tc := range testCases {
		req := NewDescribeClusterRequest(tc.version)
		if req.Version != tc.expectedVer {
			t.Fatalf("version mismatch for %v: got %d", tc.version, req.Version)
		}
		if req.Version >= 1 && req.EndpointType == 0 {
			t.Fatalf("endpoint type not set for version %d", req.Version)
		}
		if req.Version < 2 && req.IncludeFencedBrokers {
			t.Fatalf("include fenced brokers unexpectedly enabled for version %d", req.Version)
		}
	}
}

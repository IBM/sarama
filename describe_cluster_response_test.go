//go:build !functional

package sarama

import "testing"

func TestDescribeClusterResponse(t *testing.T) {
	rib := &DescribeClusterBroker{
		BrokerID: 1,
		Host:     "localhost",
		Port:     9092,
		Rack:     nullString("rack-a"),
		IsFenced: true,
	}

	resp := &DescribeClusterResponse{
		Version:                     2,
		ThrottleTimeMs:              10,
		Err:                         ErrNoError,
		EndpointType:                DescribeClusterEndpointTypeBrokers,
		ClusterID:                   "cluster-1",
		ControllerID:                1,
		Brokers:                     []*DescribeClusterBroker{rib},
		ClusterAuthorizedOperations: 7,
	}

	testResponse(t, "DescribeClusterResponse", resp, nil)
}

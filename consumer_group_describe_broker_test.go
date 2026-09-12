//go:build !functional

package sarama

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrokerConsumerGroupDescribe(t *testing.T) {
	for _, tc := range []struct {
		name            string
		kafka           KafkaVersion
		brokerMax       int16
		expectedVersion int16
	}{
		{"v0", V3_7_0_0, 0, 0},
		{"v1", V4_0_0_0, 1, 1},
		{"downgrade", V4_0_0_0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockBroker(t, 0)
			defer mock.Close()
			group := consumerGroupDescribeTestResponse(1).Groups[0]
			mock.SetHandlerByMap(map[string]MockResponse{
				"ApiVersionsRequest": NewMockApiVersionsResponse(t).SetApiKeys([]ApiVersionsResponseKey{
					{ApiKey: apiKeyConsumerGroupDescribe, MinVersion: 0, MaxVersion: tc.brokerMax},
				}),
				"ConsumerGroupDescribeRequest": NewMockConsumerGroupDescribeResponse(t).
					AddGroupDescription("g", group).
					AddGroupDescription("moved", ConsumerGroupDescription{ErrorCode: ErrNotCoordinatorForConsumer}),
			})
			conf := NewTestConfig()
			conf.Version = tc.kafka
			conf.ApiVersionsRequest = true
			broker := NewBroker(mock.Addr())
			require.NoError(t, broker.Open(conf))
			defer safeClose(t, broker)

			req := NewConsumerGroupDescribeRequest(conf.Version)
			req.GroupIDs = []string{"g", "missing", "moved"}
			req.IncludeAuthorizedOperations = true
			res, err := broker.ConsumerGroupDescribe(req)
			require.NoError(t, err)
			require.Equal(t, tc.expectedVersion, res.Version)
			require.Len(t, res.Groups, 3)
			want := consumerGroupDescribeTestResponse(tc.expectedVersion).Groups[0]
			require.Equal(t, want, res.Groups[0])
			require.Equal(t, "missing", res.Groups[1].GroupID)
			require.Equal(t, ErrGroupIDNotFound, res.Groups[1].ErrorCode)
			require.Equal(t, int32(-2147483648), res.Groups[1].AuthorizedOperations)
			require.Equal(t, ErrNotCoordinatorForConsumer, res.Groups[2].ErrorCode)

			history := mock.History()
			sent := history[len(history)-1].Request.(*ConsumerGroupDescribeRequest)
			require.Equal(t, req, sent)

			req.IncludeAuthorizedOperations = false
			res, err = broker.ConsumerGroupDescribe(req)
			require.NoError(t, err)
			require.Equal(t, int32(-2147483648), res.Groups[0].AuthorizedOperations)
		})
	}
}

func TestBrokerConsumerGroupDescribeFailure(t *testing.T) {
	// A disconnected broker returns a transport error, not an empty response.
	broker := NewBroker("localhost:9092")
	res, err := broker.ConsumerGroupDescribe(&ConsumerGroupDescribeRequest{GroupIDs: []string{"g"}})
	require.ErrorIs(t, err, ErrNotConnected)
	require.Nil(t, res)
}

func ExampleBroker_ConsumerGroupDescribe() {
	config := NewConfig()
	config.Version = V4_0_0_0
	client, err := NewClient([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()

	coordinator, err := client.Coordinator("my-group")
	if err != nil {
		panic(err)
	}
	req := NewConsumerGroupDescribeRequest(config.Version)
	req.GroupIDs = []string{"my-group"}
	res, err := coordinator.ConsumerGroupDescribe(req)
	if err != nil {
		panic(err)
	}
	for _, group := range res.Groups {
		if group.ErrorCode != ErrNoError {
			fmt.Printf("group %s: %v\n", group.GroupID, group.ErrorCode)
			continue
		}
		fmt.Printf("group %s: %d members\n", group.GroupID, len(group.Members))
	}
}

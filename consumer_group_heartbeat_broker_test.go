//go:build !functional

package sarama

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrokerConsumerGroupHeartbeat(t *testing.T) {
	for _, tc := range []struct {
		name            string
		kafka           KafkaVersion
		brokerMax       int16
		expectedVersion int16
	}{
		{"v0", V3_5_0_0, 0, 0},
		{"v1", V4_0_0_0, 1, 1},
		{"downgrade", V4_0_0_0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockBroker(t, 0)
			defer mock.Close()

			assignment := &ConsumerGroupHeartbeatAssignment{
				TopicPartitions: []ConsumerGroupHeartbeatTopicPartitions{{
					TopicID:    Uuid{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
					Partitions: []int32{0, 1},
				}},
			}

			mock.SetHandlerByMap(map[string]MockResponse{
				"ApiVersionsRequest": NewMockApiVersionsResponse(t).SetApiKeys([]ApiVersionsResponseKey{
					{ApiKey: apiKeyConsumerGroupHeartbeat, MinVersion: 0, MaxVersion: tc.brokerMax},
				}),
				"ConsumerGroupHeartbeatRequest": NewMockConsumerGroupHeartbeatResponse(t).
					SetMemberId("member-1").
					SetMemberEpoch(1).
					SetHeartbeatIntervalMs(5000).
					SetAssignment(assignment),
			})

			conf := NewTestConfig()
			conf.Version = tc.kafka
			conf.ApiVersionsRequest = true

			broker := NewBroker(mock.Addr())

			require.NoError(t, broker.Open(conf))
			defer safeClose(t, broker)

			req := NewConsumerGroupHeartbeatRequest(conf.Version)
			req.GroupId = "test-group"
			req.MemberId = "member-1"
			req.MemberEpoch = 0
			req.SubscribedTopicNames = []string{"topic1"}

			res, err := broker.ConsumerGroupHeartbeat(req)

			require.NoError(t, err)
			require.Equal(t, tc.expectedVersion, res.Version)
			require.Equal(t, ErrNoError, res.ErrorCode)
			require.Equal(t, "member-1", *res.MemberId)
			require.Equal(t, int32(1), res.MemberEpoch)
			require.Equal(t, int32(5000), res.HeartbeatIntervalMs)
			require.NotNil(t, res.Assignment)
			require.Len(t, res.Assignment.TopicPartitions, 1)
			require.Equal(t, []int32{0, 1}, res.Assignment.TopicPartitions[0].Partitions)

			history := mock.History()
			sent := history[len(history)-1].Request.(*ConsumerGroupHeartbeatRequest)
			require.Equal(t, req, sent)
		})
	}
}

func TestBrokerConsumerGroupHeartbeatFailure(t *testing.T) {
	broker := NewBroker("localhost:9092")

	res, err := broker.ConsumerGroupHeartbeat(&ConsumerGroupHeartbeatRequest{
		GroupId:  "test-group",
		MemberId: "member-1",
	})

	require.ErrorIs(t, err, ErrNotConnected)
	require.Nil(t, res)
}

func ExampleBroker_ConsumerGroupHeartbeat() {
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

	req := NewConsumerGroupHeartbeatRequest(config.Version)
	req.GroupId = "my-group"
	req.MemberId = "member-1"
	req.MemberEpoch = 0
	req.SubscribedTopicNames = []string{"my-topic"}

	res, err := coordinator.ConsumerGroupHeartbeat(req)
	if err != nil {
		panic(err)
	}

	if res.ErrorCode != ErrNoError {
		panic(res.ErrorCode)
	}

	_ = res.HeartbeatIntervalMs
}

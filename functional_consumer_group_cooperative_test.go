//go:build functional

package sarama

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const functionalCooperativeTopic = "test.4"

func TestFuncConsumerGroupCooperative(t *testing.T) {
	checkKafkaVersion(t, "2.4.0")

	t.Run("round trips messages while scaling the group", func(t *testing.T) {
		setupFunctionalTest(t)
		t.Cleanup(func() { teardownFunctionalTest(t) })

		groupID := testFuncConsumerGroupID(t)
		prefix := groupID + "-scale"
		sink := newFunctionalCooperativeSink(prefix)

		member1 := startFunctionalRoundTripMember(
			t, groupID, "cooperative-1", NewBalanceStrategyCooperativeSticky(), sink,
		)

		waitForFunctionalCooperativeMessages(t, sink,
			produceFunctionalCooperativeMessages(t, prefix, 0, 5))

		member2 := startFunctionalRoundTripMember(
			t, groupID, "cooperative-2", NewBalanceStrategyCooperativeSticky(), sink,
		)

		waitForFunctionalCooperativeMessages(t, sink,
			produceFunctionalCooperativeMessages(t, prefix, 5, 5))

		member1.AssertCleanShutdown()
		member2.AssertCleanShutdown()
	})

	t.Run("upgrades an eager group to cooperative", func(t *testing.T) {
		setupFunctionalTest(t)
		t.Cleanup(func() { teardownFunctionalTest(t) })

		groupID := testFuncConsumerGroupID(t)
		prefix := groupID + "-upgrade"
		sink := newFunctionalCooperativeSink(prefix)

		eager := startFunctionalRoundTripMember(
			t, groupID, "eager", NewBalanceStrategyRange(), sink,
		)

		waitForFunctionalCooperativeMessages(t, sink,
			produceFunctionalCooperativeMessages(t, prefix, 0, 5))
		eager.AssertCleanShutdown()

		cooperative := startFunctionalRoundTripMember(
			t, groupID, "cooperative", NewBalanceStrategyCooperativeSticky(), sink,
		)

		waitForFunctionalCooperativeMessages(t, sink,
			produceFunctionalCooperativeMessages(t, prefix, 5, 5))
		cooperative.AssertCleanShutdown()
	})
}

func startFunctionalRoundTripMember(
	t *testing.T,
	groupID string,
	clientID string,
	strategy BalanceStrategy,
	sink *testFuncConsumerGroupSink,
) *testFuncConsumerGroupMember {
	t.Helper()
	config := defaultConfig(clientID)
	config.Consumer.Offsets.Initial = OffsetOldest
	config.Consumer.Group.Heartbeat.Interval = 500 * time.Millisecond
	config.Consumer.Group.Rebalance.GroupStrategies = []BalanceStrategy{strategy}
	member := runTestFuncConsumerGroupMemberWithConfig(
		t, config, groupID, 0, sink, functionalCooperativeTopic,
	)
	t.Cleanup(member.Stop)
	member.WaitForState(2)
	return member
}

func newFunctionalCooperativeSink(prefix string) *testFuncConsumerGroupSink {
	return &testFuncConsumerGroupSink{
		msgs: make(chan testFuncConsumerGroupMessage, 100),
		filter: func(message *ConsumerMessage) bool {
			return strings.HasPrefix(string(message.Value), prefix)
		},
	}
}

func produceFunctionalCooperativeMessages(t *testing.T, prefix string, start, count int) []string {
	t.Helper()
	config := NewFunctionalTestConfig()
	config.ClientID = t.Name()
	config.Producer.Return.Successes = true
	producer, err := NewSyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer func() { require.NoError(t, producer.Close()) }()

	messages := make([]string, 0, count)
	for i := start; i < start+count; i++ {
		value := fmt.Sprintf("%s-%02d", prefix, i)
		_, _, err := producer.SendMessage(&ProducerMessage{
			Topic: functionalCooperativeTopic,
			Value: StringEncoder(value),
		})
		require.NoError(t, err)
		messages = append(messages, value)
	}
	return messages
}

func waitForFunctionalCooperativeMessages(t *testing.T, sink *testFuncConsumerGroupSink, expected []string) {
	t.Helper()
	pending := make(map[string]none, len(expected))
	for _, message := range expected {
		pending[message] = none{}
	}

	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	for len(pending) > 0 {
		select {
		case message := <-sink.msgs:
			delete(pending, string(message.Value))
		case <-timer.C:
			require.Empty(t, pending, "consumer group did not receive every message")
			return
		}
	}
}

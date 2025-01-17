//go:build !functional

package sarama

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	assert "github.com/stretchr/testify/require"
)

type handler struct {
	messageCh chan *ConsumerMessage
}

func (h *handler) Setup(s ConsumerGroupSession) error   { return nil }
func (h *handler) Cleanup(s ConsumerGroupSession) error { return nil }
func (h *handler) ConsumeClaim(sess ConsumerGroupSession, claim ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():
			sess.MarkMessage(msg, "")
			h.messageCh <- msg
		case <-sess.Context().Done():
			h.messageCh <- &ConsumerMessage{Value: []byte("session done")}
			return nil
		}
	}
}

func TestNewConsumerGroupFromClient(t *testing.T) {
	t.Run("should not permit nil client", func(t *testing.T) {
		group, err := NewConsumerGroupFromClient("group", nil)
		assert.Nil(t, group)
		assert.Error(t, err)
	})
}

// TestConsumerGroupNewSessionDuringOffsetLoad ensures that the consumer group
// will retry Join and Sync group operations, if it receives a temporary
// OffsetsLoadInProgress error response, in the same way as it would for a
// RebalanceInProgress.
func TestConsumerGroupNewSessionDuringOffsetLoad(t *testing.T) {
	config := NewTestConfig()
	config.ClientID = t.Name()
	config.Version = V2_0_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.Retry.Max = 2
	config.Consumer.Group.Rebalance.Retry.Backoff = 0
	config.Consumer.Offsets.AutoCommit.Enable = false

	broker0 := NewMockBroker(t, 0)
	defer broker0.Close()

	broker0.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("my-topic", 0, broker0.BrokerID()),
		"OffsetRequest": NewMockOffsetResponse(t).
			SetOffset("my-topic", 0, OffsetOldest, 0).
			SetOffset("my-topic", 0, OffsetNewest, 1),
		"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).
			SetCoordinator(CoordinatorGroup, "my-group", broker0),
		"HeartbeatRequest": NewMockHeartbeatResponse(t),
		"JoinGroupRequest": NewMockSequence(
			NewMockJoinGroupResponse(t).SetError(ErrOffsetsLoadInProgress),
			NewMockJoinGroupResponse(t).SetGroupProtocol(RangeBalanceStrategyName),
		),
		"SyncGroupRequest": NewMockSequence(
			NewMockSyncGroupResponse(t).SetError(ErrOffsetsLoadInProgress),
			NewMockSyncGroupResponse(t).SetMemberAssignment(
				&ConsumerGroupMemberAssignment{
					Version: 0,
					Topics: map[string][]int32{
						"my-topic": {0},
					},
				}),
		),
		"OffsetFetchRequest": NewMockOffsetFetchResponse(t).SetOffset(
			"my-group", "my-topic", 0, 0, "", ErrNoError,
		).SetError(ErrNoError),
		"FetchRequest": NewMockSequence(
			NewMockFetchResponse(t, 1).
				SetMessage("my-topic", 0, 0, StringEncoder("foo")),
			NewMockFetchResponse(t, 1).
				SetMessage("my-topic", 0, 1, StringEncoder("bar")),
		),
	})

	group, err := NewConsumerGroup([]string{broker0.Addr()}, "my-group", config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	h := &handler{make(chan *ConsumerMessage)}
	defer close(h.messageCh)

	go func() {
		topics := []string{"my-topic"}
		if err := group.Consume(ctx, topics, h); err != nil {
			t.Error(err)
		}
	}()

	assert.Equal(t, "foo", string((<-h.messageCh).Value))
	assert.Equal(t, "bar", string((<-h.messageCh).Value))
	go func() {
		if err := group.Close(); err != nil {
			t.Error(err)
		}
	}()
	assert.Equal(t, "session done", string((<-h.messageCh).Value))
}

func TestConsume_RaceTest(t *testing.T) {
	const (
		groupID     = "test-group"
		topic       = "test-topic"
		offsetStart = int64(1234)
	)

	cfg := NewTestConfig()
	cfg.Version = V2_8_1_0
	cfg.Consumer.Return.Errors = true
	cfg.Metadata.Full = true

	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	handlerMap := map[string]MockResponse{
		"ApiVersionsRequest": NewMockApiVersionsResponse(t),
		"MetadataRequest": NewMockMetadataResponse(t).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetError("mismatched-topic", ErrUnknownTopicOrPartition),
		"OffsetRequest": NewMockOffsetResponse(t).
			SetOffset(topic, 0, -1, offsetStart),
		"OffsetFetchRequest": NewMockOffsetFetchResponse(t).
			SetOffset(groupID, topic, 0, offsetStart, "", ErrNoError),
		"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).
			SetCoordinator(CoordinatorGroup, groupID, seedBroker),
		"JoinGroupRequest": NewMockJoinGroupResponse(t),
		"SyncGroupRequest": NewMockSyncGroupResponse(t).SetMemberAssignment(
			&ConsumerGroupMemberAssignment{
				Version:  1,
				Topics:   map[string][]int32{topic: {0}}, // map "test-topic" to partition 0
				UserData: []byte{0x01},
			},
		),
		"HeartbeatRequest": NewMockHeartbeatResponse(t),
	}
	seedBroker.SetHandlerByMap(handlerMap)

	cancelCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))

	retryWait := 10 * time.Millisecond
	var err error
	clientRetries := 0
outerFor:
	for {
		_, err = NewConsumerGroup([]string{seedBroker.Addr()}, groupID, cfg)
		if err == nil {
			break
		}

		if retryWait < time.Minute {
			retryWait *= 2
		}

		clientRetries++

		timer := time.NewTimer(retryWait)
		select {
		case <-cancelCtx.Done():
			err = cancelCtx.Err()
			timer.Stop()
			break outerFor
		case <-timer.C:
		}
		timer.Stop()
	}
	if err == nil {
		t.Fatalf("should not proceed to Consume")
	}

	if clientRetries <= 1 {
		t.Errorf("clientRetries = %v; want > 1", clientRetries)
	}

	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}

	cancel()
}

// TestConsumerGroupSessionDoesNotRetryForever ensures that an error fetching
// the coordinator decrements the retry attempts and doesn't end up retrying
// forever
func TestConsumerGroupSessionDoesNotRetryForever(t *testing.T) {
	config := NewTestConfig()
	config.ClientID = t.Name()
	config.Version = V2_0_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.Retry.Max = 1
	config.Consumer.Group.Rebalance.Retry.Backoff = 0

	broker0 := NewMockBroker(t, 0)
	defer broker0.Close()

	broker0.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("my-topic", 0, broker0.BrokerID()),
		"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).
			SetError(CoordinatorGroup, "my-group", ErrGroupAuthorizationFailed),
	})

	group, err := NewConsumerGroup([]string{broker0.Addr()}, "my-group", config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = group.Close() }()

	ctx := context.Background()
	h := &handler{}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		topics := []string{"my-topic"}
		err := group.Consume(ctx, topics, h)
		assert.Error(t, err)
		wg.Done()
	}()

	wg.Wait()
}

func TestConsumerShouldNotRetrySessionIfContextCancelled(t *testing.T) {
	c := &consumerGroup{
		config: NewTestConfig(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.newSession(ctx, nil, nil, 1024)
	assert.Equal(t, context.Canceled, err)
	_, err = c.retryNewSession(ctx, nil, nil, 1024, true)
	assert.Equal(t, context.Canceled, err)
}

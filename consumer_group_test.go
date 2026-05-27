//go:build !functional

package sarama

import (
	"context"
	"errors"
	"slices"
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

	if !errors.Is(err, context.DeadlineExceeded) {
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

	var consumeErr error
	go func() {
		defer wg.Done()
		topics := []string{"my-topic"}
		consumeErr = group.Consume(ctx, topics, h)
	}()

	wg.Wait()
	assert.Error(t, consumeErr)
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

// strategyWithSubscriptionUserData wraps a BalanceStrategy and adds a
// SubscriptionUserDataBalanceStrategy implementation that returns the
// configured data/error. It also records the topics it was invoked with so
// tests can assert per-cycle invocation.
type strategyWithSubscriptionUserData struct {
	BalanceStrategy
	data   []byte
	err    error
	called [][]string
}

func (s *strategyWithSubscriptionUserData) SubscriptionUserData(topics []string) ([]byte, error) {
	s.called = append(s.called, slices.Clone(topics))
	return s.data, s.err
}

func TestSubscriptionMetadata(t *testing.T) {
	staticUserData := []byte("static")
	topics := []string{"my-topic"}

	t.Run("strategy without provider uses static user data", func(t *testing.T) {
		c := &consumerGroup{userData: staticUserData}
		meta := c.subscriptionMetadata(NewBalanceStrategyRange(), topics)
		assert.Equal(t, topics, meta.Topics)
		assert.Equal(t, staticUserData, meta.UserData)
	})

	t.Run("provider returning bytes overrides static user data", func(t *testing.T) {
		c := &consumerGroup{userData: staticUserData}
		strategy := &strategyWithSubscriptionUserData{
			BalanceStrategy: NewBalanceStrategyRange(),
			data:            []byte("per-cycle"),
		}
		meta := c.subscriptionMetadata(strategy, topics)
		assert.Equal(t, []byte("per-cycle"), meta.UserData)
		assert.Equal(t, [][]string{topics}, strategy.called)
	})

	t.Run("provider returning nil clears static user data", func(t *testing.T) {
		c := &consumerGroup{userData: staticUserData}
		strategy := &strategyWithSubscriptionUserData{
			BalanceStrategy: NewBalanceStrategyRange(),
			data:            nil,
		}
		meta := c.subscriptionMetadata(strategy, topics)
		assert.Nil(t, meta.UserData)
	})

	t.Run("provider returning empty slice clears static user data", func(t *testing.T) {
		c := &consumerGroup{userData: staticUserData}
		strategy := &strategyWithSubscriptionUserData{
			BalanceStrategy: NewBalanceStrategyRange(),
			data:            []byte{},
		}
		meta := c.subscriptionMetadata(strategy, topics)
		assert.Equal(t, []byte{}, meta.UserData)
	})

	t.Run("provider returning error falls back to static user data", func(t *testing.T) {
		c := &consumerGroup{userData: staticUserData}
		strategy := &strategyWithSubscriptionUserData{
			BalanceStrategy: NewBalanceStrategyRange(),
			data:            []byte("ignored"),
			err:             errors.New("boom"),
		}
		meta := c.subscriptionMetadata(strategy, topics)
		assert.Equal(t, staticUserData, meta.UserData)
	})

	t.Run("each strategy in GroupStrategies receives its own user data", func(t *testing.T) {
		c := &consumerGroup{userData: staticUserData}
		s1 := &strategyWithSubscriptionUserData{
			BalanceStrategy: NewBalanceStrategyRange(),
			data:            []byte("from-s1"),
		}
		s2 := &strategyWithSubscriptionUserData{
			BalanceStrategy: NewBalanceStrategyRoundRobin(),
			data:            []byte("from-s2"),
		}
		assert.Equal(t, []byte("from-s1"), c.subscriptionMetadata(s1, topics).UserData)
		assert.Equal(t, []byte("from-s2"), c.subscriptionMetadata(s2, topics).UserData)
	})
}

// drainHandler is a ConsumerGroupHandler that drains messages without blocking.
type drainHandler struct{}

func (*drainHandler) Setup(_ ConsumerGroupSession) error   { return nil }
func (*drainHandler) Cleanup(_ ConsumerGroupSession) error { return nil }
func (*drainHandler) ConsumeClaim(_ ConsumerGroupSession, claim ConsumerGroupClaim) error {
	for range claim.Messages() {
	}
	return nil
}

// causeHandler is a ConsumerGroupHandler that captures the context.Cause when
// the session context is canceled.
type causeHandler struct {
	causeCh chan error
}

func (h *causeHandler) Setup(_ ConsumerGroupSession) error { return nil }
func (h *causeHandler) Cleanup(_ ConsumerGroupSession) error {
	close(h.causeCh)
	return nil
}
func (h *causeHandler) ConsumeClaim(sess ConsumerGroupSession, claim ConsumerGroupClaim) error {
	<-sess.Context().Done()
	h.causeCh <- context.Cause(sess.Context())
	return nil
}

// mockJoinGroupCapture wraps a MockJoinGroupResponse and records the Reason
// from each incoming JoinGroupRequest.
type mockJoinGroupCapture struct {
	inner    MockResponse
	mu       sync.Mutex
	reasons  []*string
	captured chan struct{} // closed when len(reasons) >= want
	want     int
}

func (m *mockJoinGroupCapture) For(reqBody versionedDecoder) encoderWithHeader {
	req := reqBody.(*JoinGroupRequest)
	m.mu.Lock()
	m.reasons = append(m.reasons, req.Reason)
	if len(m.reasons) >= m.want {
		select {
		case <-m.captured:
		default:
			close(m.captured)
		}
	}
	m.mu.Unlock()
	return m.inner.For(reqBody)
}

// mockLeaveGroupCapture wraps a MockLeaveGroupResponse and records the Reason
// from each incoming LeaveGroupRequest member.
type mockLeaveGroupCapture struct {
	inner    MockResponse
	mu       sync.Mutex
	reasons  []*string
	captured chan struct{} // closed after first LeaveGroup
}

func (m *mockLeaveGroupCapture) For(reqBody versionedDecoder) encoderWithHeader {
	req := reqBody.(*LeaveGroupRequest)
	m.mu.Lock()
	for _, member := range req.Members {
		m.reasons = append(m.reasons, member.Reason)
	}
	select {
	case <-m.captured:
	default:
		close(m.captured)
	}
	m.mu.Unlock()
	return m.inner.For(reqBody)
}

// mockHeartbeatRebalanceResponse returns ErrNoError for the first N heartbeats,
// then ErrRebalanceInProgress for all subsequent heartbeats.
type mockHeartbeatRebalanceResponse struct {
	t           TestReporter
	mu          sync.Mutex
	successLeft int
}

func (m *mockHeartbeatRebalanceResponse) For(reqBody versionedDecoder) encoderWithHeader {
	req := reqBody.(*HeartbeatRequest)
	resp := &HeartbeatResponse{Version: req.version()}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.successLeft > 0 {
		m.successLeft--
	} else {
		resp.Err = ErrRebalanceInProgress
	}
	return resp
}

func TestConsumerGroupSessionCancelCause_Rebalance(t *testing.T) {
	config := NewTestConfig()
	config.ClientID = t.Name()
	config.Version = V2_0_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.Retry.Max = 2
	config.Consumer.Group.Rebalance.Retry.Backoff = 0
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Group.Heartbeat.Interval = 50 * time.Millisecond

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
		"HeartbeatRequest": &mockHeartbeatRebalanceResponse{t: t, successLeft: 1},
		"JoinGroupRequest": NewMockJoinGroupResponse(t).SetGroupProtocol(RangeBalanceStrategyName),
		"SyncGroupRequest": NewMockSyncGroupResponse(t).SetMemberAssignment(
			&ConsumerGroupMemberAssignment{
				Version: 0,
				Topics: map[string][]int32{
					"my-topic": {0},
				},
			}),
		"OffsetFetchRequest": NewMockOffsetFetchResponse(t).SetOffset(
			"my-group", "my-topic", 0, 0, "", ErrNoError,
		).SetError(ErrNoError),
		"FetchRequest": NewMockFetchResponse(t, 1).
			SetMessage("my-topic", 0, 0, StringEncoder("foo")),
	})

	group, err := NewConsumerGroup([]string{broker0.Addr()}, "my-group", config)
	if err != nil {
		t.Fatal(err)
	}

	h := &causeHandler{causeCh: make(chan error, 1)}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	defer func() { _ = group.Close() }()

	go func() {
		_ = group.Consume(ctx, []string{"my-topic"}, h)
	}()

	var causes []error
	for cause := range h.causeCh {
		causes = append(causes, cause)
	}
	assert.Len(t, causes, 1, "expected exactly one cancellation cause")
	assert.ErrorIs(t, causes[0], ErrRebalanceInProgress)
}

func TestConsumerGroupReason(t *testing.T) {
	setup := func(t *testing.T, overrides map[string]MockResponse) (*MockBroker, ConsumerGroup) {
		t.Helper()
		config := NewTestConfig()
		config.ClientID = t.Name()
		config.Version = V3_2_0_0
		config.Consumer.Return.Errors = true
		config.Consumer.Group.Rebalance.Retry.Max = 0
		config.Consumer.Group.Rebalance.Retry.Backoff = 0
		config.Consumer.Offsets.AutoCommit.Enable = false
		config.Consumer.Group.Heartbeat.Interval = 50 * time.Millisecond

		broker0 := NewMockBroker(t, 0)
		handlers := map[string]MockResponse{
			"MetadataRequest": NewMockMetadataResponse(t).
				SetBroker(broker0.Addr(), broker0.BrokerID()).
				SetLeader("my-topic", 0, broker0.BrokerID()),
			"OffsetRequest": NewMockOffsetResponse(t).
				SetOffset("my-topic", 0, OffsetOldest, 0).
				SetOffset("my-topic", 0, OffsetNewest, 1),
			"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).
				SetCoordinator(CoordinatorGroup, "my-group", broker0),
			"HeartbeatRequest": &mockHeartbeatRebalanceResponse{t: t, successLeft: 1},
			"JoinGroupRequest": NewMockJoinGroupResponse(t).
				SetGroupProtocol(RangeBalanceStrategyName).
				SetMemberId("test-member"),
			"SyncGroupRequest": NewMockSyncGroupResponse(t).SetMemberAssignment(
				&ConsumerGroupMemberAssignment{
					Version: 0,
					Topics:  map[string][]int32{"my-topic": {0}},
				}),
			"LeaveGroupRequest": NewMockLeaveGroupResponse(t),
			"OffsetFetchRequest": NewMockOffsetFetchResponse(t).SetOffset(
				"my-group", "my-topic", 0, 0, "", ErrNoError,
			).SetError(ErrNoError),
			"FetchRequest": NewMockFetchResponse(t, 1).
				SetMessage("my-topic", 0, 0, StringEncoder("foo")),
		}
		for k, v := range overrides {
			handlers[k] = v
		}
		broker0.SetHandlerByMap(handlers)

		group, err := NewConsumerGroup([]string{broker0.Addr()}, "my-group", config)
		assert.NoError(t, err)
		return broker0, group
	}

	t.Run("JoinGroup carries reason from previous session", func(t *testing.T) {
		joinCapture := &mockJoinGroupCapture{
			inner: NewMockJoinGroupResponse(t).
				SetGroupProtocol(RangeBalanceStrategyName).
				SetMemberId("test-member"),
			captured: make(chan struct{}),
			want:     2,
		}
		broker0, group := setup(t, map[string]MockResponse{
			"JoinGroupRequest": joinCapture,
		})
		defer broker0.Close()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < 2; i++ {
				_ = group.Consume(ctx, []string{"my-topic"}, &drainHandler{})
			}
		}()

		select {
		case <-joinCapture.captured:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for second JoinGroup")
		}
		cancel()
		<-done
		_ = group.Close()

		joinCapture.mu.Lock()
		defer joinCapture.mu.Unlock()
		assert.Len(t, joinCapture.reasons, 2)
		assert.Nil(t, joinCapture.reasons[0], "first JoinGroup should have no reason")
		assert.NotNil(t, joinCapture.reasons[1], "second JoinGroup should carry reason")
		assert.Equal(t, "group is rebalancing", *joinCapture.reasons[1])
	})

	t.Run("LeaveGroup sends closing reason", func(t *testing.T) {
		leaveCapture := &mockLeaveGroupCapture{
			inner:    NewMockLeaveGroupResponse(t),
			captured: make(chan struct{}),
		}
		broker0, group := setup(t, map[string]MockResponse{
			"LeaveGroupRequest": leaveCapture,
		})
		defer broker0.Close()

		_ = group.Consume(t.Context(), []string{"my-topic"}, &drainHandler{})
		_ = group.Close()

		leaveCapture.mu.Lock()
		defer leaveCapture.mu.Unlock()
		assert.Len(t, leaveCapture.reasons, 1)
		assert.NotNil(t, leaveCapture.reasons[0], "LeaveGroup should carry reason")
		assert.Equal(t, "the consumer is being closed", *leaveCapture.reasons[0])
	})
}

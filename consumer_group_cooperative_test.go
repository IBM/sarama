//go:build !functional

package sarama

import (
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockCooperativeCoordinator returns one scripted assignment per generation
type mockCooperativeCoordinator struct {
	t TestReporter

	mu          sync.Mutex
	script      []map[string][]int32
	gen         int32
	owned       []map[string][]int32
	rebalancing bool
	fenced      bool
	protocol    string
	syncStarted chan none
	syncRelease chan none
	leaderID    string
}

// rebalanceNow makes the coordinator announce a rebalance on the next heartbeat.
func (m *mockCooperativeCoordinator) rebalanceNow() {
	m.mu.Lock()
	m.rebalancing = true
	m.mu.Unlock()
}

// fenceNow makes the coordinator forget the member.
func (m *mockCooperativeCoordinator) fenceNow() {
	m.mu.Lock()
	m.fenced = true
	m.mu.Unlock()
}

func (m *mockCooperativeCoordinator) heartbeat(reqBody versionedDecoder) encoderWithHeader {
	req := reqBody.(*HeartbeatRequest)
	m.mu.Lock()
	err := ErrNoError
	switch {
	case m.fenced:
		err = ErrUnknownMemberId
	case m.rebalancing:
		err = ErrRebalanceInProgress
	case req.GenerationId != m.gen:
		err = ErrIllegalGeneration
	}
	m.mu.Unlock()
	return &HeartbeatResponse{Version: req.Version, Err: err}
}

func newMockCooperativeCoordinator(t TestReporter, protocol string, script ...map[string][]int32) *mockCooperativeCoordinator {
	return &mockCooperativeCoordinator{t: t, script: script, protocol: protocol, leaderID: "m1"}
}

func (m *mockCooperativeCoordinator) join(reqBody versionedDecoder) encoderWithHeader {
	req := reqBody.(*JoinGroupRequest)

	m.mu.Lock()
	owned := map[string][]int32{}
	var metadata []byte
	for _, p := range req.OrderedGroupProtocols {
		if p.Name != m.protocol {
			continue
		}
		metadata = p.Metadata
		meta := &ConsumerGroupMemberMetadata{}
		err := decode(p.Metadata, meta, nil)
		assert.NoError(m.t, err) //nolint:testifylint // callbacks cannot call require through TestReporter
		for _, op := range meta.OwnedPartitions {
			owned[op.Topic] = slices.Clone(op.Partitions)
		}
	}
	m.owned = append(m.owned, owned)
	m.gen++
	m.rebalancing = false
	gen := m.gen
	m.mu.Unlock()

	return &JoinGroupResponse{
		Version:       req.Version,
		Err:           ErrNoError,
		GenerationId:  gen,
		GroupProtocol: m.protocol,
		LeaderId:      m.leaderID,
		MemberId:      "m1",
		Members:       []GroupMember{{MemberId: "m1", Metadata: metadata}},
	}
}

func (m *mockCooperativeCoordinator) sync(reqBody versionedDecoder) encoderWithHeader {
	req := reqBody.(*SyncGroupRequest)

	m.mu.Lock()
	idx := int(m.gen) - 1
	if idx >= len(m.script) {
		idx = len(m.script) - 1
	}
	assignment := m.script[idx]
	started := m.syncStarted
	release := m.syncRelease
	m.syncStarted = nil
	m.syncRelease = nil
	m.mu.Unlock()
	if started != nil {
		close(started)
		<-release
	}

	body, err := encode(&ConsumerGroupMemberAssignment{Topics: assignment}, nil)
	assert.NoError(m.t, err)
	return &SyncGroupResponse{
		Version:          req.Version,
		Err:              ErrNoError,
		MemberAssignment: body,
	}
}

func (m *mockCooperativeCoordinator) blockNextSync() (<-chan none, chan<- none) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncStarted = make(chan none)
	m.syncRelease = make(chan none)
	return m.syncStarted, m.syncRelease
}

func (m *mockCooperativeCoordinator) ownedAt(n int) map[string][]int32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n >= len(m.owned) {
		return nil
	}
	return m.owned[n]
}

func (m *mockCooperativeCoordinator) joinCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.owned)
}

type mockCooperativeFn func(versionedDecoder) encoderWithHeader

func (f mockCooperativeFn) For(reqBody versionedDecoder) encoderWithHeader { return f(reqBody) }

type trackingHandler struct {
	mu       sync.Mutex
	setups   int
	cleanups int
	entries  map[string]int
	exits    map[string]int
}

type trackingSnapshot struct {
	setups   int
	cleanups int
	entries  map[string]int
	exits    map[string]int
}

func newTrackingHandler() *trackingHandler {
	return &trackingHandler{entries: map[string]int{}, exits: map[string]int{}}
}

func (h *trackingHandler) Setup(ConsumerGroupSession) error {
	h.mu.Lock()
	h.setups++
	h.mu.Unlock()
	return nil
}

func (h *trackingHandler) Cleanup(ConsumerGroupSession) error {
	h.mu.Lock()
	h.cleanups++
	h.mu.Unlock()
	return nil
}

func (h *trackingHandler) ConsumeClaim(_ ConsumerGroupSession, claim ConsumerGroupClaim) error {
	key := fmt.Sprintf("%s/%d", claim.Topic(), claim.Partition())
	h.mu.Lock()
	h.entries[key]++
	h.mu.Unlock()

	for range claim.Messages() {
	}

	h.mu.Lock()
	h.exits[key]++
	h.mu.Unlock()
	return nil
}

func (h *trackingHandler) snapshot() trackingSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	return trackingSnapshot{
		setups:   h.setups,
		cleanups: h.cleanups,
		entries:  maps.Clone(h.entries),
		exits:    maps.Clone(h.exits),
	}
}

type returningHandler struct{}

func (*returningHandler) Setup(ConsumerGroupSession) error   { return nil }
func (*returningHandler) Cleanup(ConsumerGroupSession) error { return nil }
func (*returningHandler) ConsumeClaim(ConsumerGroupSession, ConsumerGroupClaim) error {
	return nil
}

type revocationIgnoringHandler struct {
	entered chan none
}

func (*revocationIgnoringHandler) Setup(ConsumerGroupSession) error   { return nil }
func (*revocationIgnoringHandler) Cleanup(ConsumerGroupSession) error { return nil }
func (h *revocationIgnoringHandler) ConsumeClaim(sess ConsumerGroupSession, _ ConsumerGroupClaim) error {
	h.entered <- none{}
	<-sess.Context().Done()
	return nil
}

const cooperativeTestTopic = "my-topic"

func cooperativeBrokerHandlers(t *testing.T, broker *MockBroker, coord *mockCooperativeCoordinator, partitions int32) map[string]MockResponse {
	offsets := NewMockOffsetResponse(t)
	for p := range partitions {
		offsets = offsets.SetOffset(cooperativeTestTopic, p, OffsetOldest, 0).SetOffset(cooperativeTestTopic, p, OffsetNewest, 0)
	}
	return groupBrokerHandlers(t, broker, partitions, map[string]MockResponse{
		"OffsetRequest":    offsets,
		"FetchRequest":     NewMockFetchResponse(t, 1),
		"HeartbeatRequest": mockCooperativeFn(coord.heartbeat),
		"JoinGroupRequest": mockCooperativeFn(coord.join),
		"SyncGroupRequest": mockCooperativeFn(coord.sync),
	})
}

func newCooperativeBroker(t *testing.T, coord *mockCooperativeCoordinator, partitions int32) *MockBroker {
	broker := NewMockBroker(t, 0)
	broker.SetHandlerByMap(cooperativeBrokerHandlers(t, broker, coord, partitions))
	return broker
}

func newCooperativeConfig(t *testing.T) *Config {
	config := NewTestConfig()
	config.ClientID = t.Name()
	config.Version = V3_2_0_0
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Group.Rebalance.GroupStrategies = []BalanceStrategy{NewBalanceStrategyCooperativeSticky()}
	config.Consumer.Group.Heartbeat.Interval = 20 * time.Millisecond
	config.Consumer.Group.Session.Timeout = 200 * time.Millisecond
	config.Metadata.RefreshFrequency = 0
	return config
}

func startCooperativeGroup(t *testing.T, coord *mockCooperativeCoordinator, config *Config, h ConsumerGroupHandler) (ConsumerGroup, <-chan error) {
	t.Helper()
	_, group, consumeDone := startCooperativeGroupWithPartitions(t, coord, 4, config, h)
	return group, consumeDone
}

func startCooperativeGroupWithPartitions(t *testing.T, coord *mockCooperativeCoordinator, partitions int32, config *Config, h ConsumerGroupHandler) (*MockBroker, ConsumerGroup, <-chan error) {
	t.Helper()
	broker := newCooperativeBroker(t, coord, partitions)
	t.Cleanup(broker.Close)

	if config == nil {
		config = newCooperativeConfig(t)
	}
	group, err := NewConsumerGroup([]string{broker.Addr()}, "my-group", config)
	require.NoError(t, err)

	consumeDone := make(chan error, 1)
	go func() { consumeDone <- group.Consume(t.Context(), []string{cooperativeTestTopic}, h) }()
	return broker, group, consumeDone
}

func waitForClaims(t *testing.T, h *trackingHandler, n int) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Len(c, h.snapshot().entries, n)
	}, 20*time.Second, 20*time.Millisecond)
}

func waitForJoins(t *testing.T, coord *mockCooperativeCoordinator, n int) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, coord.joinCount(), n)
	}, 20*time.Second, 20*time.Millisecond)
}

func waitForConsume(t *testing.T, consumeDone <-chan error) error {
	t.Helper()
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()

	select {
	case err := <-consumeDone:
		return err
	case <-timer.C:
		require.FailNow(t, "Consume did not return")
		return nil
	}
}

func closeCooperativeGroup(t *testing.T, group ConsumerGroup, consumeDone <-chan error) {
	t.Helper()
	require.NoError(t, group.Close())
	require.NoError(t, waitForConsume(t, consumeDone))
}

func TestConsumerGroupCooperativeRebalance(t *testing.T) {
	const topic = cooperativeTestTopic

	t.Run("a rejoining member reports the partitions it still owns", func(t *testing.T) {
		coord := newMockCooperativeCoordinator(t, CooperativeStickyBalanceStrategyName,
			map[string][]int32{topic: {0, 1, 2, 3}},
			map[string][]int32{topic: {0, 1}},
		)
		h := newTrackingHandler()
		group, consumeDone := startCooperativeGroup(t, coord, nil, h)

		waitForClaims(t, h, 4)
		coord.rebalanceNow()
		waitForJoins(t, coord, 2)

		require.Empty(t, coord.ownedAt(0), "first join should own nothing")
		owned := coord.ownedAt(1)
		require.NotNil(t, owned)
		got := slices.Clone(owned[topic])
		slices.Sort(got)
		require.Equal(t, []int32{0, 1, 2, 3}, got)

		closeCooperativeGroup(t, group, consumeDone)
	})

	t.Run("an eager member never reports owned partitions", func(t *testing.T) {
		coord := newMockCooperativeCoordinator(t, RangeBalanceStrategyName,
			map[string][]int32{topic: {0, 1, 2, 3}},
			map[string][]int32{topic: {0, 1}},
		)
		broker := newCooperativeBroker(t, coord, 4)
		t.Cleanup(broker.Close)

		config := newCooperativeConfig(t)
		config.Consumer.Group.Rebalance.GroupStrategies = []BalanceStrategy{NewBalanceStrategyRange()}
		group, err := NewConsumerGroup([]string{broker.Addr()}, "my-group", config)
		require.NoError(t, err)

		h := newTrackingHandler()
		consumeDone := make(chan error, 1)
		go func() {
			for t.Context().Err() == nil {
				if err := group.Consume(t.Context(), []string{topic}, h); err != nil {
					consumeDone <- err
					return
				}
			}
			consumeDone <- nil
		}()

		waitForClaims(t, h, 4)
		coord.rebalanceNow()
		waitForJoins(t, coord, 2)

		for i := range coord.joinCount() {
			require.Empty(t, coord.ownedAt(i), "eager join %d must not report owned partitions", i)
		}

		require.NoError(t, group.Close())
		require.ErrorIs(t, waitForConsume(t, consumeDone), ErrClosedConsumerGroup)
	})

	t.Run("retained partitions keep running across a rebalance", func(t *testing.T) {
		coord := newMockCooperativeCoordinator(t, CooperativeStickyBalanceStrategyName,
			map[string][]int32{topic: {0, 1, 2, 3}}, // generation 1
			map[string][]int32{topic: {0, 1}},       // generation 2: 2 and 3 taken away
		)
		h := newTrackingHandler()
		group, consumeDone := startCooperativeGroup(t, coord, nil, h)

		waitForClaims(t, h, 4)

		coord.rebalanceNow()

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			state := h.snapshot()
			assert.Equal(c, 1, state.exits[topic+"/2"], "partition 2 should have been revoked")
			assert.Equal(c, 1, state.exits[topic+"/3"], "partition 3 should have been revoked")
		}, 20*time.Second, 20*time.Millisecond)

		state := h.snapshot()
		require.Equal(t, 1, state.entries[topic+"/0"], "partition 0 should have been claimed exactly once")
		require.Equal(t, 1, state.entries[topic+"/1"], "partition 1 should have been claimed exactly once")
		require.Zero(t, state.exits[topic+"/0"], "partition 0 should not have been revoked")
		require.Zero(t, state.exits[topic+"/1"], "partition 1 should not have been revoked")
		require.Equal(t, 1, state.setups, "Setup should run once per member, not per generation")
		require.Zero(t, state.cleanups, "Cleanup should not run on a rebalance")

		closeCooperativeGroup(t, group, consumeDone)

		state = h.snapshot()
		require.Equal(t, 1, state.setups)
		require.Equal(t, 1, state.cleanups, "Cleanup should run once, on exit")
	})

	t.Run("a revocation makes the member rejoin without being told to", func(t *testing.T) {
		coord := newMockCooperativeCoordinator(t, CooperativeStickyBalanceStrategyName,
			map[string][]int32{topic: {0, 1, 2, 3}},
			map[string][]int32{topic: {0, 1}},
		)
		h := newTrackingHandler()
		group, consumeDone := startCooperativeGroup(t, coord, nil, h)

		waitForClaims(t, h, 4)
		coord.rebalanceNow()

		waitForJoins(t, coord, 3)

		closeCooperativeGroup(t, group, consumeDone)
	})

	t.Run("gaining partitions does not make the member rejoin", func(t *testing.T) {
		coord := newMockCooperativeCoordinator(t, CooperativeStickyBalanceStrategyName,
			map[string][]int32{topic: {0, 1}},
			map[string][]int32{topic: {0, 1, 2, 3}},
		)
		h := newTrackingHandler()
		group, consumeDone := startCooperativeGroup(t, coord, nil, h)

		waitForClaims(t, h, 2)
		coord.rebalanceNow()
		waitForClaims(t, h, 4)

		require.Never(t, func() bool { return coord.joinCount() > 2 }, 400*time.Millisecond, 25*time.Millisecond)
		require.Empty(t, h.snapshot().exits)

		closeCooperativeGroup(t, group, consumeDone)
	})

	t.Run("heartbeats wait for an in-flight rejoin to publish its generation", func(t *testing.T) {
		coord := newMockCooperativeCoordinator(t, CooperativeStickyBalanceStrategyName,
			map[string][]int32{topic: {0, 1}},
			map[string][]int32{topic: {0, 1}},
		)
		config := newCooperativeConfig(t)
		config.Consumer.Group.Heartbeat.Interval = 10 * time.Millisecond
		h := newTrackingHandler()
		group, consumeDone := startCooperativeGroup(t, coord, config, h)

		waitForClaims(t, h, 2)
		syncStarted, syncRelease := coord.blockNextSync()
		coord.rebalanceNow()

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			select {
			case <-syncStarted:
			default:
				assert.Fail(c, "SyncGroup did not start")
			}
		}, 20*time.Second, 5*time.Millisecond)

		timer := time.NewTimer(5 * config.Consumer.Group.Heartbeat.Interval)
		<-timer.C
		close(syncRelease)

		waitForJoins(t, coord, 2)
		require.Never(t, func() bool {
			select {
			case <-consumeDone:
				return true
			default:
				return false
			}
		}, 100*time.Millisecond, 5*time.Millisecond)

		closeCooperativeGroup(t, group, consumeDone)
	})

	t.Run("a ConsumeClaim returning of its own accord still ends the session", func(t *testing.T) {
		coord := newMockCooperativeCoordinator(t, CooperativeStickyBalanceStrategyName,
			map[string][]int32{topic: {0, 1}},
		)
		h := &returningHandler{}
		group, consumeDone := startCooperativeGroup(t, coord, nil, h)
		defer func() { require.NoError(t, group.Close()) }()

		require.NoError(t, waitForConsume(t, consumeDone))
	})

	t.Run("a handler that ignores revocation is cut off at the rebalance timeout", func(t *testing.T) {
		coord := newMockCooperativeCoordinator(t, CooperativeStickyBalanceStrategyName,
			map[string][]int32{topic: {0, 1}},
			map[string][]int32{topic: {0}},
		)
		config := newCooperativeConfig(t)
		config.Consumer.Group.Rebalance.Timeout = 200 * time.Millisecond

		h := &revocationIgnoringHandler{entered: make(chan none, 4)}
		group, consumeDone := startCooperativeGroup(t, coord, config, h)
		defer func() { require.NoError(t, group.Close()) }()

		<-h.entered
		<-h.entered
		coord.rebalanceNow()

		require.ErrorIs(t, waitForConsume(t, consumeDone), ErrRebalanceTimedOut)
	})

	t.Run("a member fenced by the coordinator ends the session", func(t *testing.T) {
		coord := newMockCooperativeCoordinator(t, CooperativeStickyBalanceStrategyName,
			map[string][]int32{topic: {0, 1}},
		)
		h := newTrackingHandler()
		group, consumeDone := startCooperativeGroup(t, coord, nil, h)
		defer func() { require.NoError(t, group.Close()) }()

		waitForClaims(t, h, 2)
		coord.fenceNow()

		require.NoError(t, waitForConsume(t, consumeDone))
	})

	t.Run("a partition count change makes the leader rejoin in place", func(t *testing.T) {
		coord := newMockCooperativeCoordinator(t, CooperativeStickyBalanceStrategyName,
			map[string][]int32{topic: {0, 1}},
			map[string][]int32{topic: {0, 1, 2}},
		)
		config := newCooperativeConfig(t)
		config.Metadata.RefreshFrequency = 50 * time.Millisecond
		h := newTrackingHandler()
		broker, group, consumeDone := startCooperativeGroupWithPartitions(t, coord, 2, config, h)

		waitForClaims(t, h, 2)

		broker.SetHandlerByMap(cooperativeBrokerHandlers(t, broker, coord, 3))
		waitForClaims(t, h, 3)

		state := h.snapshot()
		require.Equal(t, 1, state.setups, "the session should have survived the rebalance")
		require.Zero(t, state.cleanups)
		require.Equal(t, 1, state.entries[topic+"/0"])
		require.Equal(t, 1, state.entries[topic+"/1"])

		joins := coord.joinCount()
		require.Never(t, func() bool { return coord.joinCount() > joins }, 400*time.Millisecond, 25*time.Millisecond)

		closeCooperativeGroup(t, group, consumeDone)
	})

	t.Run("a follower refreshes metadata before claiming a new partition", func(t *testing.T) {
		coord := newMockCooperativeCoordinator(t, CooperativeStickyBalanceStrategyName,
			map[string][]int32{topic: {0, 1}},
			map[string][]int32{topic: {0, 1, 2}},
		)
		coord.leaderID = "m2"

		h := newTrackingHandler()
		broker, group, consumeDone := startCooperativeGroupWithPartitions(t, coord, 2, nil, h)

		waitForClaims(t, h, 2)

		broker.SetHandlerByMap(cooperativeBrokerHandlers(t, broker, coord, 3))
		coord.rebalanceNow()
		waitForClaims(t, h, 3)

		closeCooperativeGroup(t, group, consumeDone)
	})
}

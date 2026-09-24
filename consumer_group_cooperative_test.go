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
	memberID    string
	forgotten   bool
	heartbeats  []string

	rebalanceHeartbeats int

	ownedGens []int32  // generation reported with the owned partitions, per join
	syncErrs  []KError // errors to answer the next SyncGroup requests with
}

// failNextSync makes the coordinator answer the next SyncGroup with err.
func (m *mockCooperativeCoordinator) failNextSync(err KError) {
	m.mu.Lock()
	m.syncErrs = append(m.syncErrs, err)
	m.mu.Unlock()
}

// ownedGenAt returns the generation the nth JoinGroup reported with its
// owned partitions.
func (m *mockCooperativeCoordinator) ownedGenAt(n int) int32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n >= len(m.ownedGens) {
		return -1
	}
	return m.ownedGens[n]
}

// rebalanceNow makes the coordinator announce a rebalance on the next heartbeat.
func (m *mockCooperativeCoordinator) rebalanceNow() {
	m.mu.Lock()
	m.rebalancing = true
	m.mu.Unlock()
}

// forgetNow starts a rebalance in which the coordinator no longer knows the
// member, so its rejoin gets UNKNOWN_MEMBER_ID and it joins as a new member.
func (m *mockCooperativeCoordinator) forgetNow() {
	m.mu.Lock()
	m.forgotten = true
	m.rebalancing = true
	m.mu.Unlock()
}

// heartbeatMembers returns the member id of each heartbeat received.
func (m *mockCooperativeCoordinator) heartbeatMembers() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.heartbeats)
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
	m.heartbeats = append(m.heartbeats, req.MemberId)
	err := ErrNoError
	switch {
	case m.fenced, req.MemberId != m.memberID:
		err = ErrUnknownMemberId
	case m.rebalancing:
		err = ErrRebalanceInProgress
		m.rebalanceHeartbeats++
	case req.GenerationId != m.gen:
		err = ErrIllegalGeneration
	}
	m.mu.Unlock()
	return &HeartbeatResponse{Version: req.Version, Err: err}
}

func newMockCooperativeCoordinator(t TestReporter, protocol string, script ...map[string][]int32) *mockCooperativeCoordinator {
	return &mockCooperativeCoordinator{t: t, script: script, protocol: protocol, leaderID: "m1", memberID: "m1"}
}

func (m *mockCooperativeCoordinator) join(reqBody versionedDecoder) encoderWithHeader {
	req := reqBody.(*JoinGroupRequest)

	m.mu.Lock()
	if m.forgotten {
		if req.MemberId != "" {
			m.mu.Unlock()
			return &JoinGroupResponse{Version: req.Version, Err: ErrUnknownMemberId}
		}
		m.forgotten = false
		m.memberID = "m2"
	}
	owned := map[string][]int32{}
	ownedGen := int32(-1)
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
		ownedGen = meta.GenerationID
	}
	m.owned = append(m.owned, owned)
	m.ownedGens = append(m.ownedGens, ownedGen)
	m.gen++
	m.rebalancing = false
	gen := m.gen
	memberID := m.memberID
	m.mu.Unlock()

	return &JoinGroupResponse{
		Version:       req.Version,
		Err:           ErrNoError,
		GenerationId:  gen,
		GroupProtocol: m.protocol,
		LeaderId:      m.leaderID,
		MemberId:      memberID,
		Members:       []GroupMember{{MemberId: memberID, Metadata: metadata}},
	}
}

func (m *mockCooperativeCoordinator) sync(reqBody versionedDecoder) encoderWithHeader {
	req := reqBody.(*SyncGroupRequest)

	m.mu.Lock()
	if len(m.syncErrs) > 0 {
		err := m.syncErrs[0]
		m.syncErrs = m.syncErrs[1:]
		m.mu.Unlock()
		return &SyncGroupResponse{Version: req.Version, Err: err}
	}
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

// rebalanceHeartbeatCount returns how many heartbeats were told a rebalance
// is in progress.
func (m *mockCooperativeCoordinator) rebalanceHeartbeatCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rebalanceHeartbeats
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

// revokeHoldingHandler keeps a revoked claim's ConsumeClaim running until
// release is closed.
type revokeHoldingHandler struct {
	*trackingHandler
	revoked     chan none // closed when the first claim is revoked
	revokedOnce sync.Once
	release     chan none
}

func (h *revokeHoldingHandler) ConsumeClaim(sess ConsumerGroupSession, claim ConsumerGroupClaim) error {
	err := h.trackingHandler.ConsumeClaim(sess, claim)
	h.revokedOnce.Do(func() { close(h.revoked) })
	<-h.release
	return err
}

func newRevokeHoldingHandler() *revokeHoldingHandler {
	return &revokeHoldingHandler{
		trackingHandler: newTrackingHandler(),
		revoked:         make(chan none),
		release:         make(chan none),
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

// rebalanceAndWaitForHeartbeat starts a rebalance and waits until a heartbeat
// has been told about it.
func rebalanceAndWaitForHeartbeat(t *testing.T, coord *mockCooperativeCoordinator) {
	t.Helper()
	seen := coord.rebalanceHeartbeatCount()
	coord.rebalanceNow()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Greater(c, coord.rebalanceHeartbeatCount(), seen)
	}, 10*time.Second, 5*time.Millisecond)
}

// requireJoinsStayAt checks that the member does not join again for a while.
func requireJoinsStayAt(t *testing.T, coord *mockCooperativeCoordinator, n int) {
	t.Helper()
	require.Never(t, func() bool { return coord.joinCount() > n },
		300*time.Millisecond, 20*time.Millisecond, "the member rejoined for a rebalance it had already joined")
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

	t.Run("a rebalance seen while revoking is joined only once", func(t *testing.T) {
		coord := newMockCooperativeCoordinator(t, CooperativeStickyBalanceStrategyName,
			map[string][]int32{topic: {0, 1, 2, 3}},
			map[string][]int32{topic: {0, 1}},
		)
		h := newRevokeHoldingHandler()
		group, consumeDone := startCooperativeGroup(t, coord, nil, h)

		waitForClaims(t, h.trackingHandler, 4)
		coord.rebalanceNow()
		assertDoneWithin(t, h.revoked, 10*time.Second)

		// another member starts a rebalance while this one is still revoking
		rebalanceAndWaitForHeartbeat(t, coord)
		close(h.release)

		// the follow-up rejoin after the revoke joins that rebalance
		waitForJoins(t, coord, 3)
		requireJoinsStayAt(t, coord, 3)

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

	t.Run("a member rejoining under a new member id keeps its session", func(t *testing.T) {
		coord := newMockCooperativeCoordinator(t, CooperativeStickyBalanceStrategyName,
			map[string][]int32{},              // generation 1: nothing assigned
			map[string][]int32{topic: {0, 1}}, // generation 2: joined as m2
		)
		h := newTrackingHandler()
		group, consumeDone := startCooperativeGroup(t, coord, nil, h)

		waitForJoins(t, coord, 1)
		coord.forgetNow()
		waitForClaims(t, h, 2)

		before := len(coord.heartbeatMembers())
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			after := coord.heartbeatMembers()[before:]
			assert.NotEmpty(c, after)
			assert.NotContains(c, after, "m1", "heartbeats after the rejoin should use the new member id")
		}, 10*time.Second, 20*time.Millisecond)

		state := h.snapshot()
		require.Equal(t, 1, state.setups, "the session should survive the new member id")
		require.Zero(t, state.cleanups)

		closeCooperativeGroup(t, group, consumeDone)
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

func TestConsumerGroupCooperativeRejoinErrors(t *testing.T) {
	const topic = cooperativeTestTopic

	newConfig := func(t *testing.T) *Config {
		config := newCooperativeConfig(t)
		config.Consumer.Group.Rebalance.Retry.Backoff = 10 * time.Millisecond
		return config
	}

	t.Run("a rejoin after a failed SyncGroup reports the generation it joined", func(t *testing.T) {
		all := map[string][]int32{topic: {0, 1, 2, 3}}
		coord := newMockCooperativeCoordinator(t, CooperativeStickyBalanceStrategyName, all, all, all)
		h := newTrackingHandler()
		group, consumeDone := startCooperativeGroup(t, coord, newConfig(t), h)

		waitForClaims(t, h, 4)
		coord.failNextSync(ErrRebalanceInProgress)
		coord.rebalanceNow()
		waitForJoins(t, coord, 3)

		// the join for generation 2 succeeded before its SyncGroup failed, so
		// the member still owns its partitions in generation 2
		assert.Equal(t, int32(1), coord.ownedGenAt(1))
		assert.Equal(t, int32(2), coord.ownedGenAt(2))

		closeCooperativeGroup(t, group, consumeDone)
	})
}

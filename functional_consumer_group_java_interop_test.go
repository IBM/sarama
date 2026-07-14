//go:build functional

package sarama

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	javaCooperativeStickyAssignor = "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
	javaRangeAssignor             = "org.apache.kafka.clients.consumer.RangeAssignor"
)

// CooperativeStickyAssignor itself arrived in 2.4, but Java clients before
// 2.8.2 throw IllegalStateException from ConsumerCoordinator's
// validateCooperativeAssignment when they lead (KAFKA-13406).
const javaMinCooperativeVersion = "2.8.2"

type javaConsumerEvent struct {
	Name       string `json:"name"`
	Partitions []struct {
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
	} `json:"partitions"`
}

type assignmentTracker struct {
	t      *testing.T
	name   string
	detail func() string

	mu         sync.Mutex
	owned      map[string][]int32
	revoked    map[string][]int32
	stopReason string

	changed chan struct{}
	done    chan struct{}
	stop    sync.Once
}

func newAssignmentTracker(t *testing.T, name string) *assignmentTracker {
	return &assignmentTracker{
		t: t, name: name,
		owned:   map[string][]int32{},
		revoked: map[string][]int32{},
		changed: make(chan struct{}, 128),
		done:    make(chan struct{}),
	}
}

func (a *assignmentTracker) Name() string { return a.name }

func (a *assignmentTracker) assign(topic string, partition int32) {
	a.mu.Lock()
	if !slices.Contains(a.owned[topic], partition) {
		a.owned[topic] = append(a.owned[topic], partition)
	}
	a.mu.Unlock()
	a.notify()
}

func (a *assignmentTracker) revoke(topic string, partition int32) {
	a.mu.Lock()
	a.owned[topic] = slices.DeleteFunc(a.owned[topic], func(p int32) bool { return p == partition })
	if len(a.owned[topic]) == 0 {
		delete(a.owned, topic)
	}
	a.revoked[topic] = append(a.revoked[topic], partition)
	a.mu.Unlock()
	a.notify()
}

func (a *assignmentTracker) notify() {
	select {
	case a.changed <- struct{}{}:
	default:
	}
}

func (a *assignmentTracker) stopWith(reason string) {
	a.stop.Do(func() {
		a.mu.Lock()
		a.stopReason = reason
		a.mu.Unlock()
		close(a.done)
	})
}

func (a *assignmentTracker) Owned() map[string][]int32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return cloneAssignment(a.owned)
}

func (a *assignmentTracker) RevokedPartitions() map[string][]int32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return cloneAssignment(a.revoked)
}

func (a *assignmentTracker) waitFor(cond func(map[string][]int32) bool, what string) {
	a.t.Helper()
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	for !cond(a.Owned()) {
		select {
		case <-a.changed:
		case <-a.done:
			a.mu.Lock()
			reason := a.stopReason
			a.mu.Unlock()
			require.FailNow(a.t, fmt.Sprintf("member %s %s while waiting %s%s", a.name, reason, what, a.failureDetail()))
		case <-deadline.C:
			require.FailNow(a.t, fmt.Sprintf("timed out waiting for member %s %s, owns %v%s", a.name, what, a.Owned(), a.failureDetail()))
		}
	}
}

func (a *assignmentTracker) waitUntilJoined() {
	a.t.Helper()
	a.waitFor(func(owned map[string][]int32) bool { return len(owned) > 0 }, "to join")
}

func (a *assignmentTracker) failureDetail() string {
	if a.detail == nil {
		return ""
	}
	return a.detail()
}

type javaConsumerGroupMember struct {
	*assignmentTracker
	pidFile string
	cmd     *exec.Cmd

	stderrMu    sync.Mutex
	stderr      strings.Builder
	processDone chan error
}

func runJavaConsumerGroupMember(t *testing.T, groupID, name, topic, assignor string) *javaConsumerGroupMember {
	t.Helper()

	args := []string{
		"--bootstrap-server", brokerAddr, // in-container listener, no toxiproxy
		"--group-id", groupID,
		"--topic", topic,
		"--session-timeout", "30000",
		"--reset-policy", "earliest",
		"--assignment-strategy", assignor,
		"--verbose",
	}
	// Sarama only supports the classic group protocol
	if kafkaVersionAtLeast("3.7.0") {
		args = append(args, "--group-protocol", "classic")
	}

	// docker compose exec does not forward signals to the JVM
	pidFile := fmt.Sprintf("/tmp/sarama-java-member-%s-%d.pid", name, time.Now().UnixNano())
	script := fmt.Sprintf(
		`echo $$ > %s; exec /opt/kafka-%s/bin/kafka-verifiable-consumer.sh "$@"`,
		pidFile, FunctionalTestEnv.KafkaVersion,
	)
	cmdArgs := append([]string{
		"compose", "exec", "-T", brokerContainer, "bash", "-c", script, "bash",
	}, args...)

	m := &javaConsumerGroupMember{
		assignmentTracker: newAssignmentTracker(t, name),
		pidFile:           pidFile,
		cmd:               exec.Command("docker", cmdArgs...),
		processDone:       make(chan error, 1),
	}
	m.detail = func() string { return "; stderr:\n" + m.stderrString() }

	stdout, err := m.cmd.StdoutPipe()
	require.NoError(t, err)
	stderr, err := m.cmd.StderrPipe()
	require.NoError(t, err)
	require.NoError(t, m.cmd.Start())

	var readers sync.WaitGroup
	readers.Add(2)
	go func() {
		defer readers.Done()
		m.readEvents(stdout)
	}()
	go func() {
		defer readers.Done()
		s := bufio.NewScanner(stderr)
		for s.Scan() {
			m.stderrMu.Lock()
			m.stderr.WriteString(s.Text() + "\n")
			m.stderrMu.Unlock()
		}
		if err := s.Err(); err != nil {
			m.stopWith(fmt.Sprintf("failed to read stderr (%v)", err))
		}
	}()
	go func() {
		readers.Wait()
		err := m.cmd.Wait()
		m.processDone <- err
		m.stopWith("exited")
	}()

	t.Cleanup(func() {
		if err := m.signal("KILL"); err != nil {
			t.Logf("kill java member %s: %v", name, err)
		}
	})

	m.waitUntilJoined()
	return m
}

// readEvents derives ownership from the Java rebalance callbacks
func (m *javaConsumerGroupMember) readEvents(stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var event javaConsumerEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			m.stopWith(fmt.Sprintf("reported invalid JSON (%v)", err))
			return
		}

		switch event.Name {
		case "partitions_assigned":
			for _, p := range event.Partitions {
				m.assign(p.Topic, p.Partition)
			}
		case "partitions_revoked":
			for _, p := range event.Partitions {
				m.revoke(p.Topic, p.Partition)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		m.stopWith(fmt.Sprintf("failed to read stdout (%v)", err))
	}
}

func (m *javaConsumerGroupMember) stderrString() string {
	m.stderrMu.Lock()
	defer m.stderrMu.Unlock()
	return m.stderr.String()
}

func (m *javaConsumerGroupMember) signal(sig string) error {
	// #nosec G204 -- test-only; sig is a literal and pidFile is built here
	return exec.Command("docker", "compose", "exec", "-T", brokerContainer, "bash", "-c",
		fmt.Sprintf("kill -%s $(cat %s) 2>/dev/null || true", sig, m.pidFile)).Run()
}

func (m *javaConsumerGroupMember) StopGracefully() {
	require.NoError(m.t, m.signal("TERM"))
	select {
	case err := <-m.processDone:
		if err != nil {
			m.t.Logf("java member %s exited after SIGTERM: %v", m.name, err)
		}
	case <-time.After(60 * time.Second):
		assert.Fail(m.t, fmt.Sprintf("java member %s did not shut down", m.name))
		if err := m.signal("KILL"); err != nil {
			m.t.Logf("kill java member %s: %v", m.name, err)
		}
	}
}

func cloneAssignment(in map[string][]int32) map[string][]int32 {
	out := make(map[string][]int32, len(in))
	for topic, partitions := range in {
		sorted := slices.Clone(partitions)
		slices.Sort(sorted)
		out[topic] = sorted
	}
	return out
}

// countingStrategy records whether Sarama led a rebalance
type countingStrategy struct {
	BalanceStrategy
	mu    sync.Mutex
	plans int
}

func (s *countingStrategy) Plan(members map[string]ConsumerGroupMemberMetadata, topics map[string][]int32) (BalanceStrategyPlan, error) {
	s.mu.Lock()
	s.plans++
	s.mu.Unlock()
	return s.BalanceStrategy.Plan(members, topics)
}

func (s *countingStrategy) planCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.plans
}

func (s *countingStrategy) SupportedProtocols() []RebalanceProtocol {
	return supportedProtocols(s.BalanceStrategy)
}

func (s *countingStrategy) SubscriptionUserData(topics []string) ([]byte, error) {
	if strategy, ok := s.BalanceStrategy.(SubscriptionUserDataBalanceStrategy); ok {
		return strategy.SubscriptionUserData(topics)
	}
	return nil, nil
}

func (s *countingStrategy) OnAssignment(assignment *ConsumerGroupMemberAssignment, generationID int32) {
	if strategy, ok := s.BalanceStrategy.(OnAssignmentBalanceStrategy); ok {
		strategy.OnAssignment(assignment, generationID)
	}
}

type saramaInteropMember struct {
	*assignmentTracker
	group    ConsumerGroup
	strategy *countingStrategy

	entriesMu sync.Mutex
	entries   map[string]int
}

func runSaramaInteropMember(t *testing.T, groupID, name, topic string, primary BalanceStrategy, extra ...BalanceStrategy) *saramaInteropMember {
	t.Helper()

	counting := &countingStrategy{BalanceStrategy: primary}
	config := NewFunctionalTestConfig()
	config.ClientID = name
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = OffsetOldest
	config.Consumer.Group.Session.Timeout = 30 * time.Second
	config.Consumer.Group.Rebalance.Timeout = 30 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 2 * time.Second
	config.Consumer.Group.Rebalance.GroupStrategies = append(
		[]BalanceStrategy{counting}, extra...,
	)

	group, err := NewConsumerGroup(FunctionalTestEnv.KafkaBrokerAddrs, groupID, config)
	require.NoError(t, err)

	m := &saramaInteropMember{
		assignmentTracker: newAssignmentTracker(t, name),
		group:             group,
		strategy:          counting,
		entries:           map[string]int{},
	}

	go func() {
		for err := range group.Errors() {
			t.Logf("sarama member %s: %v", name, err)
		}
	}()

	go func() {
		for t.Context().Err() == nil {
			if err := group.Consume(t.Context(), []string{topic}, m); err != nil {
				m.stopWith(fmt.Sprintf("stopped consuming (%v)", err))
				return
			}
		}
		m.stopWith("stopped consuming (test over)")
	}()

	t.Cleanup(func() { assert.NoError(t, group.Close()) })
	m.waitUntilJoined()
	return m
}

func (m *saramaInteropMember) Setup(ConsumerGroupSession) error   { return nil }
func (m *saramaInteropMember) Cleanup(ConsumerGroupSession) error { return nil }

func (m *saramaInteropMember) ConsumeClaim(sess ConsumerGroupSession, claim ConsumerGroupClaim) error {
	m.entriesMu.Lock()
	m.entries[fmt.Sprintf("%s/%d", claim.Topic(), claim.Partition())]++
	m.entriesMu.Unlock()
	m.assign(claim.Topic(), claim.Partition())

	for msg := range claim.Messages() {
		sess.MarkMessage(msg, "")
	}

	m.revoke(claim.Topic(), claim.Partition())
	return nil
}

func (m *saramaInteropMember) claimCount(topic string, partition int32) int {
	m.entriesMu.Lock()
	defer m.entriesMu.Unlock()
	return m.entries[fmt.Sprintf("%s/%d", topic, partition)]
}

type interopGroupMember interface {
	Name() string
	Owned() map[string][]int32
	RevokedPartitions() map[string][]int32
}

var (
	_ interopGroupMember = (*javaConsumerGroupMember)(nil)
	_ interopGroupMember = (*saramaInteropMember)(nil)
)

// requireUntouched verifies that a disjoint subscription was not revoked
func requireUntouched(t *testing.T, m interopGroupMember, owned map[string][]int32) {
	t.Helper()
	require.Empty(t, m.RevokedPartitions(),
		"%s subscribes to a topic nobody else does, so an incremental rebalance must not revoke from it", m.Name())
	require.Equal(t, owned, m.Owned(), "%s should still own its partitions", m.Name())
}

// requireGroupStable tolerates transient describe errors during a rebalance
func requireGroupStable(t *testing.T, groupID, protocol string, members int) {
	t.Helper()
	admin, err := NewClusterAdmin(FunctionalTestEnv.KafkaBrokerAddrs, NewFunctionalTestConfig())
	require.NoError(t, err)
	defer func() { assert.NoError(t, admin.Close()) }()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		res, err := admin.DescribeConsumerGroups([]string{groupID})
		if !assert.NoError(c, err) || !assert.Len(c, res, 1) {
			return
		}
		desc := res[0]
		assert.Equal(c, "Stable", desc.State)
		assert.Equal(c, protocol, desc.Protocol)
		assert.Len(c, desc.Members, members)
	}, 60*time.Second, 500*time.Millisecond)
}

// TestFuncJavaInteropCooperativeRebalance tests mixed groups led by each client
func TestFuncJavaInteropCooperativeRebalance(t *testing.T) {
	skipIfExistingEnvironment(t)
	checkKafkaVersion(t, javaMinCooperativeVersion)
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	t.Run("java leads the group", func(t *testing.T) {
		groupID := testFuncConsumerGroupID(t)

		leader := runJavaConsumerGroupMember(t, groupID, "J", "test.1", javaCooperativeStickyAssignor)
		s1 := runSaramaInteropMember(t, groupID, "S1", "test.4", NewBalanceStrategyCooperativeSticky())
		s1.waitFor(func(o map[string][]int32) bool { return len(o["test.4"]) == 4 }, "to own all of test.4")

		requireGroupStable(t, groupID, CooperativeStickyBalanceStrategyName, 2)

		s2 := runSaramaInteropMember(t, groupID, "S2", "test.4", NewBalanceStrategyCooperativeSticky())
		s2.waitFor(func(o map[string][]int32) bool { return len(o["test.4"]) == 2 }, "to be given half of test.4")
		s1.waitFor(func(o map[string][]int32) bool { return len(o["test.4"]) == 2 }, "to be left half of test.4")

		require.Zero(t, s1.strategy.planCount(), "S1 should not have led the group")
		require.Zero(t, s2.strategy.planCount(), "S2 should not have led the group")

		requireUntouched(t, leader, map[string][]int32{"test.1": {0}})

		requireGroupStable(t, groupID, CooperativeStickyBalanceStrategyName, 3)
		leader.StopGracefully()
	})

	t.Run("sarama leads the group", func(t *testing.T) {
		groupID := testFuncConsumerGroupID(t)

		leader := runSaramaInteropMember(t, groupID, "S1", "test.1", NewBalanceStrategyCooperativeSticky())
		j := runJavaConsumerGroupMember(t, groupID, "J", "test.4", javaCooperativeStickyAssignor)
		j.waitFor(func(o map[string][]int32) bool { return len(o["test.4"]) == 4 }, "to own all of test.4")

		requireGroupStable(t, groupID, CooperativeStickyBalanceStrategyName, 2)

		s2 := runSaramaInteropMember(t, groupID, "S2", "test.4", NewBalanceStrategyCooperativeSticky())
		s2.waitFor(func(o map[string][]int32) bool { return len(o["test.4"]) == 2 }, "to be given half of test.4")

		require.NotZero(t, leader.strategy.planCount(), "S1 should have led the group")

		j.waitFor(func(o map[string][]int32) bool { return len(o["test.4"]) == 2 }, "to be left half of test.4")
		require.Len(t, j.RevokedPartitions()["test.4"], 2,
			"the java member should have given up exactly the 2 partitions that moved, not all 4")

		requireUntouched(t, leader, map[string][]int32{"test.1": {0}})
		require.Equal(t, 1, leader.claimCount("test.1", 0),
			"the leader's partition should have been claimed once and never re-claimed")

		requireGroupStable(t, groupID, CooperativeStickyBalanceStrategyName, 3)
		j.StopGracefully()
	})

	t.Run("falls back to eager when a member offers only an eager assignor", func(t *testing.T) {
		groupID := testFuncConsumerGroupID(t)

		leader := runJavaConsumerGroupMember(t, groupID, "J", "test.1", javaRangeAssignor)
		s1 := runSaramaInteropMember(t, groupID, "S1", "test.4",
			NewBalanceStrategyCooperativeSticky(), NewBalanceStrategyRange())
		s1.waitFor(func(o map[string][]int32) bool { return len(o["test.4"]) == 4 }, "to own all of test.4")

		requireGroupStable(t, groupID, RangeBalanceStrategyName, 2)

		s2 := runSaramaInteropMember(t, groupID, "S2", "test.4",
			NewBalanceStrategyCooperativeSticky(), NewBalanceStrategyRange())
		s2.waitFor(func(o map[string][]int32) bool { return len(o["test.4"]) == 2 }, "to be given half of test.4")

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.NotEmpty(c, leader.RevokedPartitions(),
				"under the eager protocol the java member must revoke and retake its partition")
		}, 60*time.Second, 200*time.Millisecond)

		leader.StopGracefully()
	})
}

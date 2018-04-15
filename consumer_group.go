package sarama

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ErrClosedConsumerGroup is the error returned when a method is called on a cosnumer group that has been closed.
var ErrClosedConsumerGroup = errors.New("kafka: tried to use a consumer group that was closed")

// ConsumerGroup is responsible for dividing up processing of topics and partitions
// over a collection of processes (the members of the consumer group).
type ConsumerGroup interface {
	// Subscribe joins a cluster of consumers for a given list of topics.
	// Please note that you can only run a single session at a time and must
	// close the previous session before initiating a new one.
	Subscribe(topics []string) (ConsumerGroupSession, error)

	// Close stops the ConsumerGroup and detaches any running sessions. It is required to call
	// this function before the object passes out of scope, as it will otherwise leak memory.
	Close() error
}

type consumerGroup struct {
	client    Client
	ownClient bool

	config   *Config
	groupID  string
	memberID string

	session *consumerGroupSession
	lock    sync.Mutex
	closed  uint32
}

// NewConsumerGroup creates a new consumer group the given broker addresses and configuration.
func NewConsumerGroup(addrs []string, groupID string, config *Config) (ConsumerGroup, error) {
	client, err := NewClient(addrs, config)
	if err != nil {
		return nil, err
	}

	c := NewConsumerGroupFromClient(groupID, client)
	c.(*consumerGroup).ownClient = true
	return c, nil
}

// NewConsumerFromClient creates a new consumer group using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this consumer.
// PLEASE NOTE: consumer groups can only re-use but not share clients.
func NewConsumerGroupFromClient(groupID string, client Client) ConsumerGroup {
	return &consumerGroup{
		client:  client,
		config:  client.Config(),
		groupID: groupID,
	}
}

// Close implements ConsumerGroup.
func (c *consumerGroup) Close() (err error) {
	if !atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		return
	}

	c.lock.Lock()
	session := c.session
	c.session = nil
	c.lock.Unlock()

	if session != nil {
		if e := session.Release(); e != nil {
			err = e
		}
	}

	if e := c.leave(); e != nil {
		err = e
	}

	if c.ownClient {
		if e := c.client.Close(); e != nil {
			err = e
		}
	}

	return
}

// Subscribe implements ConsumerGroup.
func (c *consumerGroup) Subscribe(topics []string) (ConsumerGroupSession, error) {
	// Ensure group is not closed
	if c.isClosed() {
		return nil, ErrClosedConsumerGroup
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// Ensure there is no session running
	if c.session != nil {
		return nil, fmt.Errorf("a session is already running")
	}

	// Quick exit when no topics are provided
	if len(topics) == 0 {
		return nil, fmt.Errorf("no topics provided")
	}

	if err := c.client.RefreshMetadata(topics...); err != nil {
		return nil, err
	}
	if err := c.client.RefreshCoordinator(c.groupID); err != nil {
		return nil, err
	}

	coordinator, err := c.client.Coordinator(c.groupID)
	if err != nil {
		return nil, err
	}

	session, err := c.createSession(coordinator, topics, 0)
	if err != nil {
		return nil, err
	}

	c.session = session
	return session, nil
}

func (c *consumerGroup) isClosed() bool {
	return atomic.LoadUint32(&c.closed) != 0
}

func (c *consumerGroup) createSession(coordinator *Broker, topics []string, retry int) (*consumerGroupSession, error) {
	// Abort retries if closed
	if c.isClosed() {
		return nil, ErrClosedConsumerGroup
	}

	// Join consumer group
	join, err := c.joinGroupRequest(coordinator, topics)
	if err != nil {
		_ = coordinator.Close()
		return nil, err
	}
	switch join.Err {
	case ErrNoError:
		c.memberID = join.MemberId
	case ErrUnknownMemberId: // reset member ID and retry
		c.memberID = ""
		return c.createSession(coordinator, topics, retry)
	default:
		return nil, join.Err
	}

	// Prepare distribution plan if we joined as the leader
	var plan BalanceStrategyPlan
	if join.LeaderId == join.MemberId {
		members, err := join.GetMembers()
		if err != nil {
			return nil, err
		}

		plan, err = c.balance(members)
		if err != nil {
			return nil, err
		}
	}

	// Sync consumer group
	sync, err := c.syncGroupRequest(coordinator, plan, join.GenerationId)
	if err != nil {
		_ = coordinator.Close()
		return nil, err
	}
	switch sync.Err {
	case ErrNoError:
	case ErrRebalanceInProgress:
		if retry < c.config.Consumer.Group.Rebalance.Retry.Max {
			time.Sleep(c.config.Consumer.Group.Rebalance.Retry.Backoff)
			return c.createSession(coordinator, topics, retry+1)
		}
		return nil, sync.Err
	default:
		return nil, sync.Err
	}

	// Retrieve and sort claims
	members, err := sync.GetMemberAssignment()
	if err != nil {
		return nil, err
	}
	for topic := range members.Topics {
		sort.Sort(int32Slice(members.Topics[topic]))
	}

	// Init session
	session := &consumerGroupSession{
		claims:       members.Topics,
		parent:       c,
		generationID: join.GenerationId,

		closing: make(chan none),
		done:    make(chan struct{}),
	}
	go session.loop()
	return session, nil
}

func (c *consumerGroup) joinGroupRequest(coordinator *Broker, topics []string) (*JoinGroupResponse, error) {
	req := &JoinGroupRequest{
		GroupId:        c.groupID,
		MemberId:       c.memberID,
		SessionTimeout: int32(c.config.Consumer.Group.Session.Timeout / time.Millisecond),
		ProtocolType:   "consumer",
	}
	if c.config.Version.IsAtLeast(V0_10_1_0) {
		req.Version = 1
		req.RebalanceTimeout = int32(c.config.Consumer.Group.Rebalance.Timeout / time.Millisecond)
	}

	meta := &ConsumerGroupMemberMetadata{
		Topics:   topics,
		UserData: c.config.Consumer.Group.Member.UserData,
	}
	strategy := c.config.Consumer.Group.Rebalance.Strategy
	if err := req.AddGroupProtocolMetadata(strategy.Name(), meta); err != nil {
		return nil, err
	}

	return coordinator.JoinGroup(req)
}

func (c *consumerGroup) syncGroupRequest(coordinator *Broker, plan BalanceStrategyPlan, generationID int32) (*SyncGroupResponse, error) {
	req := &SyncGroupRequest{
		GroupId:      c.groupID,
		MemberId:     c.memberID,
		GenerationId: generationID,
	}
	for memberID, topics := range plan {
		err := req.AddGroupAssignmentMember(memberID, &ConsumerGroupMemberAssignment{
			Topics: topics,
		})
		if err != nil {
			return nil, err
		}
	}
	return coordinator.SyncGroup(req)
}

func (c *consumerGroup) balance(members map[string]ConsumerGroupMemberMetadata) (BalanceStrategyPlan, error) {
	topics := make(map[string][]int32)
	for _, meta := range members {
		for _, topic := range meta.Topics {
			topics[topic] = nil
		}
	}

	for topic := range topics {
		partitions, err := c.client.Partitions(topic)
		if err != nil {
			return nil, err
		}
		topics[topic] = partitions
	}

	strategy := c.config.Consumer.Group.Rebalance.Strategy
	return strategy.Plan(members, topics)
}

func (c *consumerGroup) heartbeat(generationID int32) error {
	coordinator, err := c.client.Coordinator(c.groupID)
	if err != nil {
		return err
	}

	resp, err := coordinator.Heartbeat(&HeartbeatRequest{
		GroupId:      c.groupID,
		MemberId:     c.memberID,
		GenerationId: generationID,
	})
	if err != nil {
		_ = coordinator.Close()
		return err
	}

	switch resp.Err {
	case ErrNoError:
		return nil
	default:
		return resp.Err
	}
}

// Leaves the cluster, called by Close, protected by lock.
func (c *consumerGroup) leave() error {
	if c.memberID == "" {
		return nil
	}

	coordinator, err := c.client.Coordinator(c.groupID)
	if err != nil {
		return err
	}

	resp, err := coordinator.LeaveGroup(&LeaveGroupRequest{
		GroupId:  c.groupID,
		MemberId: c.memberID,
	})
	if err != nil {
		_ = coordinator.Close()
		return err
	}

	// Unset memberID
	c.memberID = ""

	// Check response
	switch resp.Err {
	case ErrRebalanceInProgress, ErrUnknownMemberId, ErrNoError:
		return nil
	default:
		return resp.Err
	}
}

// --------------------------------------------------------------------

// ConsumerGroupSession represents a consumer group member session.
type ConsumerGroupSession interface {
	// Claims returns the claimed partitions by topic.
	Claims() map[string][]int32
	// MemberID returns the cluster member ID.
	MemberID() string
	// GenerationID returns the current generation ID.
	GenerationID() int32
	// Done exposes a notification channel to signal the end of the session
	// and to indicate that a new rebalance cycle is due. The session will
	// continue to hold existing claims until Close() is called, to allow
	// all tasks to flush any pending data and commit offsets.
	// Please note that brokers will wait for no more than
	// Consumer.Group.Rebalance.Timeout before evicting the worker from
	// the group.
	Done() <-chan struct{}
	// Release aborts the session and releases the claims.
	Release() error
}

type consumerGroupSession struct {
	claims map[string][]int32
	parent *consumerGroup

	generationID int32

	closing     chan none
	done        chan struct{}
	lastError   error
	releaseOnce sync.Once
}

func (s *consumerGroupSession) Claims() map[string][]int32 { return s.claims }
func (s *consumerGroupSession) MemberID() string           { return s.parent.memberID }
func (s *consumerGroupSession) GenerationID() int32        { return s.generationID }
func (s *consumerGroupSession) Done() <-chan struct{}      { return s.done }

func (s *consumerGroupSession) Release() (err error) {
	s.releaseOnce.Do(func() {
		close(s.closing)
		<-s.done

		err = s.lastError
		s.parent.lock.Lock()
		s.parent.session = nil
		s.parent.lock.Unlock()
	})
	return
}

func (s *consumerGroupSession) loop() {
	defer close(s.done)

	heartbeat := time.NewTicker(s.parent.config.Consumer.Group.Heartbeat.Interval)
	defer heartbeat.Stop()

	for {
		select {
		case <-heartbeat.C:
			if err := s.parent.heartbeat(s.generationID); err != nil {
				switch err {
				case ErrRebalanceInProgress:
				default:
					s.lastError = err
				}
				return
			}
		case <-s.closing:
			return
		}
	}
}

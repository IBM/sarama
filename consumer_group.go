package sarama

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rcrowley/go-metrics"
)

// ErrClosedConsumerGroup is the error returned when a method is called on a consumer group that has been closed.
var ErrClosedConsumerGroup = errors.New("kafka: tried to use a consumer group that was closed")

// ConsumerGroup is responsible for dividing up processing of topics and partitions
// over a collection of processes (the members of the consumer group).
type ConsumerGroup interface {
	// Consume joins a cluster of consumers for a given list of topics and
	// starts a blocking ConsumerGroupSession through the ConsumerGroupHandler.
	//
	// The life-cycle of a session is represented by the following steps:
	//
	// 1. The consumers join the group (as explained in https://kafka.apache.org/documentation/#intro_consumers)
	//    and is assigned their "fair share" of partitions, aka 'claims'.
	// 2. Before processing starts, the handler's Setup() hook is called to notify the user
	//    of the claims and allow any necessary preparation or alteration of state.
	// 3. For each of the assigned claims the handler's ConsumeClaim() function is then called
	//    in a separate goroutine which requires it to be thread-safe. Any state must be carefully protected
	//    from concurrent reads/writes.
	// 4. The session will persist until one of the ConsumeClaim() functions exits. This can be either when the
	//    parent context is canceled or when a server-side rebalance cycle is initiated.
	// 5. Once all the ConsumeClaim() loops have exited, the handler's Cleanup() hook is called
	//    to allow the user to perform any final tasks before a rebalance.
	// 6. Finally, marked offsets are committed one last time before claims are released.
	//
	// Please note, that once a rebalance is triggered, sessions must be completed within
	// Config.Consumer.Group.Rebalance.Timeout. This means that ConsumeClaim() functions must exit
	// as quickly as possible to allow time for Cleanup() and the final offset commit. If the timeout
	// is exceeded, the consumer will be removed from the group by Kafka, which will cause offset
	// commit failures.
	// This method should be called inside an infinite loop, when a
	// server-side rebalance happens, the consumer session will need to be
	// recreated to get the new claims.
	Consume(ctx context.Context, topics []string, handler ConsumerGroupHandler) error

	// Errors returns a read channel of errors that occurred during the consumer life-cycle.
	// By default, errors are logged and not returned over this channel.
	// If you want to implement any custom error handling, set your config's
	// Consumer.Return.Errors setting to true, and read from this channel.
	Errors() <-chan error

	// Close stops the ConsumerGroup and detaches any running sessions. It is required to call
	// this function before the object passes out of scope, as it will otherwise leak memory.
	Close() error

	// Pause suspends fetching from the requested partitions. Future calls to the broker will not return any
	// records from these partitions until they have been resumed using Resume()/ResumeAll().
	// Note that this method does not affect partition subscription.
	// In particular, it does not cause a group rebalance when automatic assignment is used.
	Pause(partitions map[string][]int32)

	// Resume resumes specified partitions which have been paused with Pause()/PauseAll().
	// New calls to the broker will return records from these partitions if there are any to be fetched.
	Resume(partitions map[string][]int32)

	// Pause suspends fetching from all partitions. Future calls to the broker will not return any
	// records from these partitions until they have been resumed using Resume()/ResumeAll().
	// Note that this method does not affect partition subscription.
	// In particular, it does not cause a group rebalance when automatic assignment is used.
	PauseAll()

	// Resume resumes all partitions which have been paused with Pause()/PauseAll().
	// New calls to the broker will return records from these partitions if there are any to be fetched.
	ResumeAll()
}

type consumerGroup struct {
	client Client

	config          *Config
	consumer        Consumer
	groupID         string
	groupInstanceId *string
	memberID        string
	errors          chan error

	lock       sync.Mutex
	errorsLock sync.RWMutex
	closed     chan none
	closeOnce  sync.Once

	userData            []byte
	rebalanceInProgress chan none

	metricRegistry metrics.Registry
	loopPartCheck  atomic.Bool
}

// NewConsumerGroup creates a new consumer group the given broker addresses and configuration.
func NewConsumerGroup(addrs []string, groupID string, config *Config) (ConsumerGroup, error) {
	client, err := NewClient(addrs, config)
	if err != nil {
		return nil, err
	}

	c, err := newConsumerGroup(groupID, client)
	if err != nil {
		_ = client.Close()
	}
	return c, err
}

// NewConsumerGroupFromClient creates a new consumer group using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this consumer.
// PLEASE NOTE: consumer groups can only re-use but not share clients.
func NewConsumerGroupFromClient(groupID string, client Client) (ConsumerGroup, error) {
	if client == nil {
		return nil, ConfigurationError("client must not be nil")
	}
	// For clients passed in by the client, ensure we don't
	// call Close() on it.
	cli := &nopCloserClient{client}
	return newConsumerGroup(groupID, cli)
}

func newConsumerGroup(groupID string, client Client) (ConsumerGroup, error) {
	config := client.Config()
	if !config.Version.IsAtLeast(V0_10_2_0) {
		return nil, ConfigurationError("consumer groups require Version to be >= V0_10_2_0")
	}

	consumer, err := newConsumer(client)
	if err != nil {
		return nil, err
	}

	cg := &consumerGroup{
		client:              client,
		consumer:            consumer,
		config:              config,
		groupID:             groupID,
		errors:              make(chan error, config.ChannelBufferSize),
		closed:              make(chan none),
		userData:            config.Consumer.Group.Member.UserData,
		metricRegistry:      newCleanupRegistry(config.MetricRegistry),
		rebalanceInProgress: make(chan none),
	}
	if config.Consumer.Group.InstanceId != "" && config.Version.IsAtLeast(V2_3_0_0) {
		cg.groupInstanceId = &config.Consumer.Group.InstanceId
	}
	return cg, nil
}

// Errors implements ConsumerGroup.
func (c *consumerGroup) Errors() <-chan error { return c.errors }

// Close implements ConsumerGroup.
func (c *consumerGroup) Close() (err error) {
	c.closeOnce.Do(func() {
		close(c.closed)

		// leave group
		if e := c.leave(); e != nil {
			err = e
		}

		go func() {
			c.errorsLock.Lock()
			defer c.errorsLock.Unlock()
			close(c.errors)
		}()

		// drain errors
		for e := range c.errors {
			err = e
		}

		if e := c.client.Close(); e != nil {
			err = e
		}

		c.metricRegistry.UnregisterAll()
	})
	return
}

// Consume implements ConsumerGroup.
func (c *consumerGroup) Consume(ctx context.Context, topics []string, handler ConsumerGroupHandler) error {
	// Ensure group is not closed
	select {
	case <-c.closed:
		return ErrClosedConsumerGroup
	default:
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// Quick exit when no topics are provided
	if len(topics) == 0 {
		return fmt.Errorf("no topics provided")
	}

	// Refresh metadata for requested topics
	if err := c.client.RefreshMetadata(topics...); err != nil {
		return err
	}

	// Init session
	sess, err := c.newSession(ctx, topics, handler, c.config.Consumer.Group.Rebalance.Retry.Max)
	if errors.Is(err, ErrClosedClient) {
		return ErrClosedConsumerGroup
	} else if err != nil {
		return err
	}

	if c.config.Consumer.Group.Rebalance.IsIncremental {
		go c.incRebalanceLoop(ctx, topics, handler, c.config.Consumer.Group.Rebalance.Retry.Max, sess)
	}

	// Wait for session exit signal or Close() call
	select {
	case <-c.closed:
	case <-sess.ctx.Done():
	}

	// Gracefully release session claims
	return sess.release(true)
}

func (c *consumerGroup) incRebalanceLoop(ctx context.Context, topics []string, handler ConsumerGroupHandler, retries int, sess *consumerGroupSession) {
	for {
		if _, ok := <-sess.rebalanceInProgress; !ok {
			Logger.Printf("closed rebalanceInProgress\n")
			return
		}

		err := c.IncRebalance(ctx, topics, handler, retries, sess)
		if err != nil {
			Logger.Printf("Error during incremental rebalance: %v", err)
			sess.cancel()
			return
		}
	}
}

// Pause implements ConsumerGroup.
func (c *consumerGroup) Pause(partitions map[string][]int32) {
	c.consumer.Pause(partitions)
}

// Resume implements ConsumerGroup.
func (c *consumerGroup) Resume(partitions map[string][]int32) {
	c.consumer.Resume(partitions)
}

// PauseAll implements ConsumerGroup.
func (c *consumerGroup) PauseAll() {
	c.consumer.PauseAll()
}

// ResumeAll implements ConsumerGroup.
func (c *consumerGroup) ResumeAll() {
	c.consumer.ResumeAll()
}

func (c *consumerGroup) retryNewSession(ctx context.Context, topics []string, handler ConsumerGroupHandler, retries int, refreshCoordinator bool) (*consumerGroupSession, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, ErrClosedConsumerGroup
	case <-time.After(c.config.Consumer.Group.Rebalance.Retry.Backoff):
	}

	if refreshCoordinator {
		err := c.client.RefreshCoordinator(c.groupID)
		if err != nil {
			if retries <= 0 {
				return nil, err
			}
			return c.retryNewSession(ctx, topics, handler, retries-1, true)
		}
	}

	return c.newSession(ctx, topics, handler, retries-1)
}

func (c *consumerGroup) newSession(ctx context.Context, topics []string, handler ConsumerGroupHandler, retries int) (*consumerGroupSession, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	coordinator, err := c.client.Coordinator(c.groupID)
	if err != nil {
		if retries <= 0 {
			return nil, err
		}

		return c.retryNewSession(ctx, topics, handler, retries, true)
	}

	var (
		metricRegistry          = c.metricRegistry
		consumerGroupJoinTotal  metrics.Counter
		consumerGroupJoinFailed metrics.Counter
		consumerGroupSyncTotal  metrics.Counter
		consumerGroupSyncFailed metrics.Counter
	)

	if metricRegistry != nil {
		consumerGroupJoinTotal = metrics.GetOrRegisterCounter(fmt.Sprintf("consumer-group-join-total-%s", c.groupID), metricRegistry)
		consumerGroupJoinFailed = metrics.GetOrRegisterCounter(fmt.Sprintf("consumer-group-join-failed-%s", c.groupID), metricRegistry)
		consumerGroupSyncTotal = metrics.GetOrRegisterCounter(fmt.Sprintf("consumer-group-sync-total-%s", c.groupID), metricRegistry)
		consumerGroupSyncFailed = metrics.GetOrRegisterCounter(fmt.Sprintf("consumer-group-sync-failed-%s", c.groupID), metricRegistry)
	}

	// Join consumer group
	join, err := c.joinGroupRequest(coordinator, topics)
	if consumerGroupJoinTotal != nil {
		consumerGroupJoinTotal.Inc(1)
	}
	if err != nil {
		_ = coordinator.Close()
		if consumerGroupJoinFailed != nil {
			consumerGroupJoinFailed.Inc(1)
		}
		return nil, err
	}
	if !errors.Is(join.Err, ErrNoError) {
		if consumerGroupJoinFailed != nil {
			consumerGroupJoinFailed.Inc(1)
		}
	}
	switch join.Err {
	case ErrNoError:
		c.memberID = join.MemberId
	case ErrUnknownMemberId, ErrIllegalGeneration:
		// reset member ID and retry immediately
		c.memberID = ""
		return c.newSession(ctx, topics, handler, retries)
	case ErrNotCoordinatorForConsumer, ErrRebalanceInProgress, ErrOffsetsLoadInProgress:
		// retry after backoff
		if retries <= 0 {
			return nil, join.Err
		}
		return c.retryNewSession(ctx, topics, handler, retries, true)
	case ErrMemberIdRequired:
		// from JoinGroupRequest v4 onwards (due to KIP-394) if the client starts
		// with an empty member id, it needs to get the assigned id from the
		// response and send another join request with that id to actually join the
		// group
		c.memberID = join.MemberId
		return c.newSession(ctx, topics, handler, retries)
	case ErrFencedInstancedId:
		if c.groupInstanceId != nil {
			Logger.Printf("JoinGroup failed: group instance id %s has been fenced\n", *c.groupInstanceId)
		}
		return nil, join.Err
	default:
		return nil, join.Err
	}

	var strategy BalanceStrategy
	var ok bool
	if strategy = c.config.Consumer.Group.Rebalance.Strategy; strategy == nil {
		strategy, ok = c.findStrategy(join.GroupProtocol, c.config.Consumer.Group.Rebalance.GroupStrategies)
		if !ok {
			// this case shouldn't happen in practice, since the leader will choose the protocol
			// that all the members support
			return nil, fmt.Errorf("unable to find selected strategy: %s", join.GroupProtocol)
		}
	}

	// Prepare distribution plan if we joined as the leader
	var plan BalanceStrategyPlan
	var members map[string]ConsumerGroupMemberMetadata
	var allSubscribedTopicPartitions map[string][]int32
	var allSubscribedTopics []string
	if join.LeaderId == join.MemberId {
		members, err = join.GetMembers()
		if err != nil {
			return nil, err
		}

		allSubscribedTopicPartitions, allSubscribedTopics, plan, err = c.balance(strategy, members)
		if err != nil {
			return nil, err
		}
	}

	// Sync consumer group
	syncGroupResponse, err := c.syncGroupRequest(coordinator, members, plan, join.GenerationId, strategy)
	if consumerGroupSyncTotal != nil {
		consumerGroupSyncTotal.Inc(1)
	}
	if err != nil {
		_ = coordinator.Close()
		if consumerGroupSyncFailed != nil {
			consumerGroupSyncFailed.Inc(1)
		}
		return nil, err
	}
	if !errors.Is(syncGroupResponse.Err, ErrNoError) {
		if consumerGroupSyncFailed != nil {
			consumerGroupSyncFailed.Inc(1)
		}
	}

	switch syncGroupResponse.Err {
	case ErrNoError:
	case ErrUnknownMemberId, ErrIllegalGeneration:
		// reset member ID and retry immediately
		c.memberID = ""
		return c.newSession(ctx, topics, handler, retries)
	case ErrNotCoordinatorForConsumer, ErrRebalanceInProgress, ErrOffsetsLoadInProgress:
		// retry after backoff
		if retries <= 0 {
			return nil, syncGroupResponse.Err
		}
		return c.retryNewSession(ctx, topics, handler, retries, true)
	case ErrFencedInstancedId:
		if c.groupInstanceId != nil {
			Logger.Printf("JoinGroup failed: group instance id %s has been fenced\n", *c.groupInstanceId)
		}
		return nil, syncGroupResponse.Err
	default:
		return nil, syncGroupResponse.Err
	}

	// Retrieve and sort claims
	var claims map[string][]int32
	if len(syncGroupResponse.MemberAssignment) > 0 {
		members, err := syncGroupResponse.GetMemberAssignment()
		if err != nil {
			return nil, err
		}
		claims = members.Topics

		// in the case of stateful balance strategies, hold on to the returned
		// assignment metadata, otherwise, reset the statically defined consumer
		// group metadata
		if members.UserData != nil {
			c.userData = members.UserData
		} else {
			c.userData = c.config.Consumer.Group.Member.UserData
		}

		for _, partitions := range claims {
			sort.Sort(int32Slice(partitions))
		}
	}

	session, err := newConsumerGroupSession(ctx, c, claims, join.MemberId, join.GenerationId, handler)
	if err != nil {
		return nil, err
	}

	// only the leader needs to check whether there are newly-added partitions in order to trigger a rebalance
	if join.LeaderId == join.MemberId {
		go c.loopCheckPartitionNumbers(allSubscribedTopicPartitions, allSubscribedTopics, session)
	}

	return session, err
}

func membersFromJoinResp(joinResp *JoinGroupResponse) (map[string]ConsumerGroupMemberMetadata, error) {
	// Prepare distribution plan if we joined as the leader
	var members map[string]ConsumerGroupMemberMetadata
	var err error
	if joinResp.LeaderId == joinResp.MemberId {
		members, err = joinResp.GetMembers()
		if err != nil {
			return nil, err
		}
	}

	return members, nil
}

func (c *consumerGroup) leaderDistributionPlan(joinResp *JoinGroupResponse, strategy BalanceStrategy, members map[string]ConsumerGroupMemberMetadata) (map[string][]int32, []string, BalanceStrategyPlan, error) {
	// Prepare distribution plan if we joined as the leader
	var plan BalanceStrategyPlan
	var allSubscribedTopicPartitions map[string][]int32
	var allSubscribedTopics []string
	var err error
	if joinResp.LeaderId == joinResp.MemberId {
		allSubscribedTopicPartitions, allSubscribedTopics, plan, err = c.balance(strategy, members)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	return allSubscribedTopicPartitions, allSubscribedTopics, plan, nil
}

func (c *consumerGroup) strategy(joinResp *JoinGroupResponse) (BalanceStrategy, error) {
	var strategy BalanceStrategy
	var ok bool
	if strategy = c.config.Consumer.Group.Rebalance.Strategy; strategy == nil {
		strategy, ok = c.findStrategy(joinResp.GroupProtocol, c.config.Consumer.Group.Rebalance.GroupStrategies)
		if !ok {
			// this case shouldn't happen in practice, since the leader will choose the protocol
			// that all the members support
			return nil, fmt.Errorf("unable to find selected strategy: %s", joinResp.GroupProtocol)
		}
	}

	return strategy, nil
}

func (c *consumerGroup) joinIncRebalanceReq(coordinator *Broker, topics []string) (*JoinGroupResponse, error) {
	// Join consumer group
	var (
		metricRegistry          = c.metricRegistry
		consumerGroupJoinTotal  metrics.Counter
		consumerGroupJoinFailed metrics.Counter
	)

	if metricRegistry != nil {
		consumerGroupJoinTotal = metrics.GetOrRegisterCounter(fmt.Sprintf("consumer-group-join-total-%s", c.groupID), metricRegistry)
		consumerGroupJoinFailed = metrics.GetOrRegisterCounter(fmt.Sprintf("consumer-group-join-failed-%s", c.groupID), metricRegistry)
	}

	join, err := c.joinGroupRequest(coordinator, topics)
	if consumerGroupJoinTotal != nil {
		consumerGroupJoinTotal.Inc(1)
	}
	if err != nil {
		_ = coordinator.Close()
		if consumerGroupJoinFailed != nil {
			consumerGroupJoinFailed.Inc(1)
		}
		return nil, err
	}
	if !errors.Is(join.Err, ErrNoError) {
		if consumerGroupJoinFailed != nil {
			consumerGroupJoinFailed.Inc(1)
		}
	}

	if join.Err != ErrNoError {
		return nil, join.Err
	}

	return join, nil
}

func (c *consumerGroup) SyncGroupIncRebalanceReq(
	coordinator *Broker,
	members map[string]ConsumerGroupMemberMetadata,
	generationID int32,
	plan BalanceStrategyPlan,
	strategy BalanceStrategy,
) (*SyncGroupResponse, error) {
	var (
		metricRegistry          = c.metricRegistry
		consumerGroupSyncTotal  metrics.Counter
		consumerGroupSyncFailed metrics.Counter
	)

	if metricRegistry != nil {
		consumerGroupSyncTotal = metrics.GetOrRegisterCounter(fmt.Sprintf("consumer-group-sync-total-%s", c.groupID), metricRegistry)
		consumerGroupSyncFailed = metrics.GetOrRegisterCounter(fmt.Sprintf("consumer-group-sync-failed-%s", c.groupID), metricRegistry)
	}

	// Sync consumer group
	syncGroupResponse, err := c.syncGroupRequest(coordinator, members, plan, generationID, strategy)
	if consumerGroupSyncTotal != nil {
		consumerGroupSyncTotal.Inc(1)
	}
	if err != nil {
		_ = coordinator.Close()
		if consumerGroupSyncFailed != nil {
			consumerGroupSyncFailed.Inc(1)
		}
		return nil, err
	}
	if !errors.Is(syncGroupResponse.Err, ErrNoError) {
		if consumerGroupSyncFailed != nil {
			consumerGroupSyncFailed.Inc(1)
		}
	}

	switch syncGroupResponse.Err {
	case ErrNoError:
	case ErrUnknownMemberId, ErrIllegalGeneration:
		return nil, syncGroupResponse.Err
	case ErrNotCoordinatorForConsumer, ErrRebalanceInProgress, ErrOffsetsLoadInProgress:
		// retry after backoff
		return nil, syncGroupResponse.Err

	case ErrFencedInstancedId:
		if c.groupInstanceId != nil {
			Logger.Printf("SyncGroupp failed: group instance id %s has been fenced\n", *c.groupInstanceId)
		}
		return nil, syncGroupResponse.Err
	default:
		return nil, syncGroupResponse.Err
	}

	return syncGroupResponse, nil
}

type ReJoinResponse struct {
	join                         *JoinGroupResponse
	sync                         *SyncGroupResponse
	allSubscribedTopicPartitions map[string][]int32
	allSubscribedTopics          []string
}

func (c *consumerGroup) ReJoin(
	_ context.Context,
	coordinator *Broker,
	topics []string,
	sess *consumerGroupSession,
) (*ReJoinResponse, error) {
	sess.Commit()

	join, err := c.joinIncRebalanceReq(coordinator, topics)

	if err != nil {
		return nil, err
	}

	strategy, err := c.strategy(join)

	if err != nil {
		return nil, err
	}

	members, err := membersFromJoinResp(join)

	if err != nil {
		return nil, err
	}

	allSubscribedTopicPartitions, allSubscribedTopics, plan, err := c.leaderDistributionPlan(join, strategy, members)
	if err != nil {
		return nil, err
	}

	sess.updateGenerationID(join.GenerationId)
	sess.offsets.updateGeneration(join.GenerationId)

	syncGroupResponse, err := c.SyncGroupIncRebalanceReq(coordinator, members, join.GenerationId, plan, strategy)
	if err != nil {
		return nil, err
	}

	return &ReJoinResponse{
		join:                         join,
		sync:                         syncGroupResponse,
		allSubscribedTopicPartitions: allSubscribedTopicPartitions,
		allSubscribedTopics:          allSubscribedTopics,
	}, nil
}

func (c *consumerGroup) claimsFromSyncResp(syncResponse *SyncGroupResponse) (map[string][]int32, error) {
	var claims map[string][]int32
	if len(syncResponse.MemberAssignment) > 0 {
		members, err := syncResponse.GetMemberAssignment()
		if err != nil {
			return nil, err
		}
		claims = members.Topics

		if members.UserData != nil {
			c.userData = members.UserData
		} else {
			c.userData = c.config.Consumer.Group.Member.UserData
		}
	}

	return claims, nil
}

func (c *consumerGroup) IncRebalance(
	ctx context.Context,
	topics []string,
	handler ConsumerGroupHandler,
	retries int,
	sess *consumerGroupSession,
) error {
	Logger.Printf("start incremental rebalance\n")
	defer func() {
		Logger.Printf("end incremental rebalance\n")
	}()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	coordinator, err := c.client.Coordinator(c.groupID)
	if err != nil {
		return err
	}

	reJoinResp, err := c.ReJoin(context.TODO(), coordinator, topics, sess)
	if err != nil {
		if retries <= 0 {
			return err
		}

		Logger.Printf("Rejoin failed, retry request\n")

		return c.IncRebalance(ctx, topics, handler, retries-1, sess)
	}

	sess.Commit()

	err = c.rebalanceIncClaims(reJoinResp, sess)

	return err
}

func (c *consumerGroup) rebalanceIncClaims(reJoinResp *ReJoinResponse, sess *consumerGroupSession) error {
	claims, err := c.claimsFromSyncResp(reJoinResp.sync)

	if err != nil {
		return err
	}

	// difference between the current claims and the previous claims
	removedClaims, addedClaims := diffClaims(sess.claims, claims)

	Logger.Printf("new claims: %v\n", claims)
	sess.incStartNewClaims(reJoinResp.join.GenerationId, claims, removedClaims, addedClaims)

	Logger.Printf("allSubscribedTopicPartitions rebalance %v\n", reJoinResp.allSubscribedTopicPartitions)

	//// only the leader needs to check whether there are newly-added partitions in order to trigger a rebalance
	if reJoinResp.join.LeaderId == reJoinResp.join.MemberId {
		Logger.Printf("I am leader %s %s\n", sess.parent.config.ClientID, reJoinResp.join.MemberId)
		go c.loopCheckPartitionNumbers(reJoinResp.allSubscribedTopicPartitions, reJoinResp.allSubscribedTopics, sess)
	}

	return nil
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
	if c.config.Version.IsAtLeast(V0_11_0_0) {
		req.Version = 2
	}
	if c.config.Version.IsAtLeast(V0_11_0_0) {
		req.Version = 2
	}
	if c.config.Version.IsAtLeast(V2_0_0_0) {
		req.Version = 3
	}
	// from JoinGroupRequest v4 onwards (due to KIP-394) the client will actually
	// send two JoinGroupRequests, once with the empty member id, and then again
	// with the assigned id from the first response. This is handled via the
	// ErrMemberIdRequired case.
	if c.config.Version.IsAtLeast(V2_2_0_0) {
		req.Version = 4
	}
	if c.config.Version.IsAtLeast(V2_3_0_0) {
		req.Version = 5
		req.GroupInstanceId = c.groupInstanceId
	}

	meta := &ConsumerGroupMemberMetadata{
		Topics:   topics,
		UserData: c.userData,
	}
	var strategy BalanceStrategy
	if strategy = c.config.Consumer.Group.Rebalance.Strategy; strategy != nil {
		if err := req.AddGroupProtocolMetadata(strategy.Name(), meta); err != nil {
			return nil, err
		}
	} else {
		for _, strategy = range c.config.Consumer.Group.Rebalance.GroupStrategies {
			if err := req.AddGroupProtocolMetadata(strategy.Name(), meta); err != nil {
				return nil, err
			}
		}
	}

	return coordinator.JoinGroup(req)
}

// findStrategy returns the BalanceStrategy with the specified protocolName
// from the slice provided.
func (c *consumerGroup) findStrategy(name string, groupStrategies []BalanceStrategy) (BalanceStrategy, bool) {
	for _, strategy := range groupStrategies {
		if strategy.Name() == name {
			return strategy, true
		}
	}
	return nil, false
}

func (c *consumerGroup) syncGroupRequest(
	coordinator *Broker,
	members map[string]ConsumerGroupMemberMetadata,
	plan BalanceStrategyPlan,
	generationID int32,
	strategy BalanceStrategy,
) (*SyncGroupResponse, error) {
	req := &SyncGroupRequest{
		GroupId:      c.groupID,
		MemberId:     c.memberID,
		GenerationId: generationID,
	}

	// Versions 1 and 2 are the same as version 0.
	if c.config.Version.IsAtLeast(V0_11_0_0) {
		req.Version = 1
	}
	if c.config.Version.IsAtLeast(V2_0_0_0) {
		req.Version = 2
	}
	// Starting from version 3, we add a new field called groupInstanceId to indicate member identity across restarts.
	if c.config.Version.IsAtLeast(V2_3_0_0) {
		req.Version = 3
		req.GroupInstanceId = c.groupInstanceId
	}

	for memberID, topics := range plan {
		assignment := &ConsumerGroupMemberAssignment{Topics: topics}
		userDataBytes, err := strategy.AssignmentData(memberID, topics, generationID)
		if err != nil {
			return nil, err
		}
		assignment.UserData = userDataBytes
		if err := req.AddGroupAssignmentMember(memberID, assignment); err != nil {
			return nil, err
		}
		delete(members, memberID)
	}
	// add empty assignments for any remaining members
	for memberID := range members {
		if err := req.AddGroupAssignmentMember(memberID, &ConsumerGroupMemberAssignment{}); err != nil {
			return nil, err
		}
	}

	return coordinator.SyncGroup(req)
}

func (c *consumerGroup) heartbeatRequest(coordinator *Broker, memberID string, generationID int32) (*HeartbeatResponse, error) {
	req := &HeartbeatRequest{
		GroupId:      c.groupID,
		MemberId:     memberID,
		GenerationId: generationID,
	}

	// Version 1 and version 2 are the same as version 0.
	if c.config.Version.IsAtLeast(V0_11_0_0) {
		req.Version = 1
	}
	if c.config.Version.IsAtLeast(V2_0_0_0) {
		req.Version = 2
	}
	// Starting from version 3, we add a new field called groupInstanceId to indicate member identity across restarts.
	if c.config.Version.IsAtLeast(V2_3_0_0) {
		req.Version = 3
		req.GroupInstanceId = c.groupInstanceId
	}

	return coordinator.Heartbeat(req)
}

func (c *consumerGroup) balance(strategy BalanceStrategy, members map[string]ConsumerGroupMemberMetadata) (map[string][]int32, []string, BalanceStrategyPlan, error) {
	topicPartitions := make(map[string][]int32)
	for _, meta := range members {
		for _, topic := range meta.Topics {
			topicPartitions[topic] = nil
		}
	}

	allSubscribedTopics := make([]string, 0, len(topicPartitions))
	for topic := range topicPartitions {
		allSubscribedTopics = append(allSubscribedTopics, topic)
	}

	// refresh metadata for all the subscribed topics in the consumer group
	// to avoid using stale metadata to assigning partitions
	err := c.client.RefreshMetadata(allSubscribedTopics...)
	if err != nil {
		return nil, nil, nil, err
	}

	for topic := range topicPartitions {
		partitions, err := c.client.Partitions(topic)
		if err != nil {
			return nil, nil, nil, err
		}
		topicPartitions[topic] = partitions
	}

	plan, err := strategy.Plan(members, topicPartitions)
	return topicPartitions, allSubscribedTopics, plan, err
}

// Leaves the cluster, called by Close.
func (c *consumerGroup) leave() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.memberID == "" {
		return nil
	}

	coordinator, err := c.client.Coordinator(c.groupID)
	if err != nil {
		return err
	}

	// as per KIP-345 if groupInstanceId is set, i.e. static membership is in action, then do not leave group when consumer closed, just clear memberID
	if c.groupInstanceId != nil {
		c.memberID = ""
		return nil
	}
	req := &LeaveGroupRequest{
		GroupId:  c.groupID,
		MemberId: c.memberID,
	}
	if c.config.Version.IsAtLeast(V0_11_0_0) {
		req.Version = 1
	}
	if c.config.Version.IsAtLeast(V2_0_0_0) {
		req.Version = 2
	}
	if c.config.Version.IsAtLeast(V2_4_0_0) {
		req.Version = 3
		req.Members = append(req.Members, MemberIdentity{
			MemberId: c.memberID,
		})
	}

	resp, err := coordinator.LeaveGroup(req)
	if err != nil {
		_ = coordinator.Close()
		return err
	}

	// clear the memberID
	c.memberID = ""

	switch resp.Err {
	case ErrRebalanceInProgress, ErrUnknownMemberId, ErrNoError:
		return nil
	default:
		return resp.Err
	}
}

func (c *consumerGroup) handleError(err error, topic string, partition int32) {
	var consumerError *ConsumerError
	if ok := errors.As(err, &consumerError); !ok && topic != "" && partition > -1 {
		err = &ConsumerError{
			Topic:     topic,
			Partition: partition,
			Err:       fmt.Errorf("%w consumerGroup error", err),
		}
	}

	if !c.config.Consumer.Return.Errors {
		Logger.Println(err)
		return
	}

	c.errorsLock.RLock()
	defer c.errorsLock.RUnlock()
	select {
	case <-c.closed:
		// consumer is closed
		return
	default:
	}

	select {
	case c.errors <- err:
	default:
		// no error listener
	}
}

func (c *consumerGroup) loopCheckPartitionNumbers(allSubscribedTopicPartitions map[string][]int32, topics []string, session *consumerGroupSession) {
	if !c.loopPartCheck.CompareAndSwap(false, true) {
		return
	}

	defer c.loopPartCheck.Store(false)

	if c.config.Metadata.RefreshFrequency == time.Duration(0) {
		return
	}

	defer session.cancel()

	oldTopicToPartitionNum := make(map[string]int, len(allSubscribedTopicPartitions))
	for topic, partitions := range allSubscribedTopicPartitions {
		oldTopicToPartitionNum[topic] = len(partitions)
	}

	pause := time.NewTicker(c.config.Metadata.RefreshFrequency)
	defer pause.Stop()
	for {
		if newTopicToPartitionNum, err := c.topicToPartitionNumbers(topics); err != nil {
			return
		} else {
			for topic, num := range oldTopicToPartitionNum {
				if newTopicToPartitionNum[topic] != num {
					Logger.Printf(
						"consumergroup/%s loop check partition number goroutine find partitions in topics %s changed from %d to %d\n",
						c.groupID, topics, num, newTopicToPartitionNum[topic])
					return // trigger the end of the session on exit
				}
			}
		}
		select {
		case <-pause.C:
		case <-session.ctx.Done():
			Logger.Printf(
				"consumergroup/%s loop check partition number goroutine will exit, topics %s\n",
				c.groupID, topics)
			// if session closed by other, should be exited
			return
		case <-c.closed:
			return
		}
	}
}

func (c *consumerGroup) topicToPartitionNumbers(topics []string) (map[string]int, error) {
	topicToPartitionNum := make(map[string]int, len(topics))
	for _, topic := range topics {
		if partitionNum, err := c.client.Partitions(topic); err != nil {
			Logger.Printf(
				"consumergroup/%s topic %s get partition number failed due to '%v'\n",
				c.groupID, topic, err)
			return nil, err
		} else {
			topicToPartitionNum[topic] = len(partitionNum)
		}
	}
	return topicToPartitionNum, nil
}

// --------------------------------------------------------------------

// ConsumerGroupSession represents a consumer group member session.
type ConsumerGroupSession interface {
	// Claims returns information about the claimed partitions by topic.
	Claims() map[string][]int32

	// MemberID returns the cluster member ID.
	MemberID() string

	// GenerationID returns the current generation ID.
	GenerationID() int32

	// MarkOffset marks the provided offset, alongside a metadata string
	// that represents the state of the partition consumer at that point in time. The
	// metadata string can be used by another consumer to restore that state, so it
	// can resume consumption.
	//
	// To follow upstream conventions, you are expected to mark the offset of the
	// next message to read, not the last message read. Thus, when calling `MarkOffset`
	// you should typically add one to the offset of the last consumed message.
	//
	// Note: calling MarkOffset does not necessarily commit the offset to the backend
	// store immediately for efficiency reasons, and it may never be committed if
	// your application crashes. This means that you may end up processing the same
	// message twice, and your processing should ideally be idempotent.
	MarkOffset(topic string, partition int32, offset int64, metadata string)

	// Commit the offset to the backend
	//
	// Note: calling Commit performs a blocking synchronous operation.
	Commit()

	// ResetOffset resets to the provided offset, alongside a metadata string that
	// represents the state of the partition consumer at that point in time. Reset
	// acts as a counterpart to MarkOffset, the difference being that it allows to
	// reset an offset to an earlier or smaller value, where MarkOffset only
	// allows incrementing the offset. cf MarkOffset for more details.
	ResetOffset(topic string, partition int32, offset int64, metadata string)

	// MarkMessage marks a message as consumed.
	MarkMessage(msg *ConsumerMessage, metadata string)

	// Context returns the session context.
	Context() context.Context
}

type consumerGroupSession struct {
	mu           sync.RWMutex
	parent       *consumerGroup
	memberID     string
	generationID int32
	handler      ConsumerGroupHandler

	claims        map[string][]int32
	claimsBrokers map[string]map[int32]*consumerGroupClaim
	offsets       *offsetManager
	ctx           context.Context
	cancel        func()

	waitGroup           sync.WaitGroup
	releaseOnce         sync.Once
	hbDying, hbDead     chan none
	rebalanceInProgress chan none
}

func newConsumerGroupSession(ctx context.Context, parent *consumerGroup, claims map[string][]int32, memberID string, generationID int32, handler ConsumerGroupHandler) (*consumerGroupSession, error) {
	// init context
	ctx, cancel := context.WithCancel(ctx)

	// init offset manager
	offsets, err := newOffsetManagerFromClient(parent.groupID, memberID, generationID, parent.client, cancel)
	if err != nil {
		return nil, err
	}

	// init session
	sess := &consumerGroupSession{
		parent:              parent,
		memberID:            memberID,
		generationID:        generationID,
		handler:             handler,
		offsets:             offsets,
		claims:              claims,
		claimsBrokers:       make(map[string]map[int32]*consumerGroupClaim),
		ctx:                 ctx,
		cancel:              cancel,
		hbDying:             make(chan none),
		hbDead:              make(chan none),
		rebalanceInProgress: make(chan none),
	}

	// start heartbeat loop
	go sess.heartbeatLoop()

	// create a POM for each claim
	for topic, partitions := range claims {
		for _, partition := range partitions {
			pom, err := offsets.ManagePartition(topic, partition)
			if err != nil {
				_ = sess.release(false)
				return nil, err
			}

			// handle POM errors
			go func(topic string, partition int32) {
				for err := range pom.Errors() {
					sess.parent.handleError(err, topic, partition)
				}
			}(topic, partition)
		}
	}

	// perform setup
	if err := handler.Setup(sess); err != nil {
		_ = sess.release(true)
		return nil, err
	}

	// start consuming
	for topic, partitions := range claims {
		for _, partition := range partitions {
			sess.waitGroup.Add(1)

			go func(topic string, partition int32) {
				defer sess.waitGroup.Done()

				// cancel the as session as soon as the first
				// goroutine exits
				if !sess.parent.config.Consumer.Group.Rebalance.IsIncremental {
					defer sess.cancel()
				}

				// consume a single topic/partition, blocking
				sess.consume(topic, partition)
			}(topic, partition)
		}
	}
	return sess, nil
}

func (s *consumerGroupSession) Claims() map[string][]int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.claims
}

func (s *consumerGroupSession) MemberID() string { return s.memberID }
func (s *consumerGroupSession) GenerationID() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.generationID
}

func (s *consumerGroupSession) updateGenerationID(generationID int32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.generationID = generationID
}

func (s *consumerGroupSession) updateClaims(claims map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.claims = claims
}

func (s *consumerGroupSession) addBrokerClaim(topic string, partition int32, claim *consumerGroupClaim) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.claimsBrokers[topic]; !ok {
		s.claimsBrokers[topic] = make(map[int32]*consumerGroupClaim)
	}
	s.claimsBrokers[topic][partition] = claim
}

func (s *consumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	if pom := s.offsets.findPOM(topic, partition); pom != nil {
		pom.MarkOffset(offset, metadata)
	}
}

func (s *consumerGroupSession) Commit() {
	s.offsets.Commit()
}

func (s *consumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	if pom := s.offsets.findPOM(topic, partition); pom != nil {
		pom.ResetOffset(offset, metadata)
	}
}

func (s *consumerGroupSession) MarkMessage(msg *ConsumerMessage, metadata string) {
	s.MarkOffset(msg.Topic, msg.Partition, msg.Offset+1, metadata)
}

func (s *consumerGroupSession) Context() context.Context {
	return s.ctx
}

func (s *consumerGroupSession) consume(topic string, partition int32) {
	// quick exit if rebalance is due
	select {
	case <-s.ctx.Done():
		return
	case <-s.parent.closed:
		return
	default:
	}

	// get next offset
	offset := s.parent.config.Consumer.Offsets.Initial
	if pom := s.offsets.findPOM(topic, partition); pom != nil {
		offset, _ = pom.NextOffset()
	}

	// create new claim
	claim, err := newConsumerGroupClaim(s, topic, partition, offset)
	if err != nil {
		s.parent.handleError(err, topic, partition)
		return
	}

	s.addBrokerClaim(topic, partition, claim)

	// handle errors
	go func() {
		for err := range claim.Errors() {
			s.parent.handleError(err, topic, partition)
		}
	}()

	// trigger close when session is done
	go func() {
		select {
		case <-s.ctx.Done():
		case <-s.parent.closed:
		}
		claim.AsyncClose()
	}()

	// start processing
	if err := s.handler.ConsumeClaim(s, claim); err != nil {
		s.parent.handleError(err, topic, partition)
	}

	// ensure consumer is closed & drained
	claim.AsyncClose()
	for _, err := range claim.waitClosed() {
		s.parent.handleError(err, topic, partition)
	}
}

func (s *consumerGroupSession) release(withCleanup bool) (err error) {
	// signal release, stop heartbeat
	s.cancel()

	// wait for consumers to exit
	s.waitGroup.Wait()

	// perform release
	s.releaseOnce.Do(func() {
		if withCleanup {
			if e := s.handler.Cleanup(s); e != nil {
				s.parent.handleError(e, "", -1)
				err = e
			}
		}

		if e := s.offsets.Close(); e != nil {
			err = e
		}

		close(s.hbDying)
		<-s.hbDead
	})

	Logger.Printf(
		"consumergroup/session/%s/%d released\n",
		s.MemberID(), s.GenerationID())

	return
}

func (s *consumerGroupSession) heartbeatLoop() {
	defer close(s.hbDead)
	defer s.cancel() // trigger the end of the session on exit
	defer func() {
		Logger.Printf(
			"consumergroup/session/%s/%d heartbeat loop stopped\n",
			s.MemberID(), s.GenerationID())
	}()

	defer close(s.rebalanceInProgress)

	pause := time.NewTicker(s.parent.config.Consumer.Group.Heartbeat.Interval)
	defer pause.Stop()

	retryBackoff := time.NewTimer(s.parent.config.Metadata.Retry.Backoff)
	defer retryBackoff.Stop()

	retries := s.parent.config.Metadata.Retry.Max
	for {
		coordinator, err := s.parent.client.Coordinator(s.parent.groupID)
		if err != nil {
			if retries <= 0 {
				s.parent.handleError(err, "", -1)
				return
			}
			retryBackoff.Reset(s.parent.config.Metadata.Retry.Backoff)
			select {
			case <-s.hbDying:
				return
			case <-retryBackoff.C:
				retries--
			}
			continue
		}

		resp, err := s.parent.heartbeatRequest(coordinator, s.memberID, s.generationID)
		if err != nil {
			_ = coordinator.Close()

			if retries <= 0 {
				s.parent.handleError(err, "", -1)
				return
			}

			retries--
			continue
		}

		switch resp.Err {
		case ErrNoError:
			retries = s.parent.config.Metadata.Retry.Max
		case ErrRebalanceInProgress:
			retries = s.parent.config.Metadata.Retry.Max
			s.rebalanceInProgress <- none{}
			if !s.parent.config.Consumer.Group.Rebalance.IsIncremental {
				s.cancel()
			}
		case ErrUnknownMemberId, ErrIllegalGeneration:
			return
		case ErrFencedInstancedId:
			if s.parent.groupInstanceId != nil {
				Logger.Printf("JoinGroup failed: group instance id %s has been fenced\n", *s.parent.groupInstanceId)
			}
			s.parent.handleError(resp.Err, "", -1)
			return
		default:
			s.parent.handleError(resp.Err, "", -1)
			return
		}

		select {
		case <-pause.C:
		case <-s.hbDying:
			return
		}
	}
}
func (s *consumerGroupSession) removeClaimAndPOMBrokers(removedClaims map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for topic, removePartitions := range removedClaims {
		if claimsBroker, ok := s.claimsBrokers[topic]; ok {
			for part, claim := range claimsBroker {
				if slices.Contains(removePartitions, part) {
					err := claim.Close()
					if err != nil {
						s.parent.handleError(fmt.Errorf("%w erorr close claim", err), topic, part)
						continue
					}
					Logger.Printf("closed claim, pom for topic %s, part %d", topic, part)
				}
			}
		}
	}

	s.Commit()

	for topic, removePartitions := range removedClaims {
		if claimsBroker, ok := s.claimsBrokers[topic]; ok {
			for part := range claimsBroker {
				if slices.Contains(removePartitions, part) {
					delete(s.claimsBrokers[topic], part)
					if pom := s.offsets.findPOM(topic, part); pom != nil {
						err := pom.Close()
						if err != nil {
							s.parent.handleError(fmt.Errorf("%w erorr close POM", err), topic, part)
							continue
						}
						Logger.Printf("closed POM for topic %s, part %d", topic, part)
					}
				}
			}
			if len(s.claimsBrokers[topic]) == 0 {
				delete(s.claimsBrokers, topic)
			}
		}
	}

	s.Commit()
}

func (s *consumerGroupSession) createNewPOMs(addedClaims map[string][]int32) {
	// create a POM for each claim
	for topic, partitions := range addedClaims {
		for _, partition := range partitions {
			pom, err := s.offsets.ManagePartition(topic, partition)
			if err != nil {
				_ = s.release(false)
				Logger.Printf("error creating partition offset manager: %s\n", err)
				return
			}

			// handle POM errors
			go func(topic string, partition int32) {
				for err := range pom.Errors() {
					s.parent.handleError(err, topic, partition)
				}
			}(topic, partition)
		}
	}
}

func (s *consumerGroupSession) startClaims(addedClaims map[string][]int32) {
	// start consuming
	for topic, partitions := range addedClaims {
		for _, partition := range partitions {
			s.waitGroup.Add(1)

			go func(topic string, partition int32) {
				defer s.waitGroup.Done()

				// consume a single topic/partition, blocking
				Logger.Printf("start consuming new topic %s, partition %d\n", topic, partition)
				s.consume(topic, partition)
			}(topic, partition)
		}
	}
}

func (s *consumerGroupSession) incStartNewClaims(generationID int32, claims, removedClaims, addedClaims map[string][]int32) {
	s.updateClaims(claims)
	s.removeClaimAndPOMBrokers(removedClaims)
	s.createNewPOMs(addedClaims)

	if err := s.handler.Setup(s); err != nil {
		_ = s.release(true)
		Logger.Printf("error handler Setup: %s\n", err)
		return
	}

	s.startClaims(addedClaims)
}

// --------------------------------------------------------------------

// ConsumerGroupHandler instances are used to handle individual topic/partition claims.
// It also provides hooks for your consumer group session life-cycle and allow you to
// trigger logic before or after the consume loop(s).
//
// PLEASE NOTE that handlers are likely be called from several goroutines concurrently,
// ensure that all state is safely protected against race conditions.
type ConsumerGroupHandler interface {
	// Setup is run at the beginning of a new session, before ConsumeClaim.
	Setup(ConsumerGroupSession) error

	// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
	// but before the offsets are committed for the very last time.
	Cleanup(ConsumerGroupSession) error

	// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
	// Once the Messages() channel is closed, the Handler must finish its processing
	// loop and exit.
	ConsumeClaim(ConsumerGroupSession, ConsumerGroupClaim) error
}

// ConsumerGroupClaim processes Kafka messages from a given topic and partition within a consumer group.
type ConsumerGroupClaim interface {
	// Topic returns the consumed topic name.
	Topic() string

	// Partition returns the consumed partition.
	Partition() int32

	// InitialOffset returns the initial offset that was used as a starting point for this claim.
	InitialOffset() int64

	// HighWaterMarkOffset returns the high watermark offset of the partition,
	// i.e. the offset that will be used for the next message that will be produced.
	// You can use this to determine how far behind the processing is.
	HighWaterMarkOffset() int64

	// Messages returns the read channel for the messages that are returned by
	// the broker. The messages channel will be closed when a new rebalance cycle
	// is due. You must finish processing and mark offsets within
	// Config.Consumer.Group.Session.Timeout before the topic/partition is eventually
	// re-assigned to another group member.
	Messages() <-chan *ConsumerMessage
}

type consumerGroupClaim struct {
	topic     string
	partition int32
	offset    int64
	PartitionConsumer
}

func newConsumerGroupClaim(sess *consumerGroupSession, topic string, partition int32, offset int64) (*consumerGroupClaim, error) {
	pcm, err := sess.parent.consumer.ConsumePartition(topic, partition, offset)

	if errors.Is(err, ErrOffsetOutOfRange) && sess.parent.config.Consumer.Group.ResetInvalidOffsets {
		offset = sess.parent.config.Consumer.Offsets.Initial
		pcm, err = sess.parent.consumer.ConsumePartition(topic, partition, offset)
	}
	if err != nil {
		return nil, err
	}

	go func() {
		for err := range pcm.Errors() {
			sess.parent.handleError(err, topic, partition)
		}
	}()

	return &consumerGroupClaim{
		topic:             topic,
		partition:         partition,
		offset:            offset,
		PartitionConsumer: pcm,
	}, nil
}

func (c *consumerGroupClaim) Topic() string        { return c.topic }
func (c *consumerGroupClaim) Partition() int32     { return c.partition }
func (c *consumerGroupClaim) InitialOffset() int64 { return c.offset }

// Drains messages and errors, ensures the claim is fully closed.
func (c *consumerGroupClaim) waitClosed() (errs ConsumerErrors) {
	go func() {
		for range c.Messages() {
		}
	}()

	for err := range c.Errors() {
		errs = append(errs, err)
	}
	return
}

func diffClaims(oldClaims, newClaims map[string][]int32) (removedClaims, addedClaims map[string][]int32) {
	removedClaims = make(map[string][]int32)
	addedClaims = make(map[string][]int32)

	for topic, partitions := range oldClaims {
		if newPartitions, ok := newClaims[topic]; ok {
			removed, added := diffPartitions(partitions, newPartitions)
			if len(removed) > 0 {
				removedClaims[topic] = removed
			}
			if len(added) > 0 {
				addedClaims[topic] = added
			}
		} else {
			removedClaims[topic] = partitions
		}
	}

	for topic, partitions := range newClaims {
		if _, ok := oldClaims[topic]; !ok {
			addedClaims[topic] = partitions
		}
	}

	return
}

func diffPartitions(partitions, newPartitions []int32) (removed, added []int32) {
	removed = make([]int32, 0)
	added = make([]int32, 0)

	for _, partition := range partitions {
		if !slices.Contains(newPartitions, partition) {
			removed = append(removed, partition)
		}
	}

	for _, partition := range newPartitions {
		if !slices.Contains(partitions, partition) {
			added = append(added, partition)
		}
	}

	return
}

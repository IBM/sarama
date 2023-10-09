package sarama

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
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

	// ConsumeV2 joins a cluster of consumers for a given list of topics and ConsumerGroupHandlerV2.
	// It should be wrapped in an infinite loop so that it would join the group again after a rebalance.
	//
	// Normally, you should always pass the same topic list and ConsumerGroupHandlerV2 instance in ConsumeV2,
	// unless you want to change the subscribed topics or the handler implementation during the lifetime of the consumer group.
	//
	// Unlike the above Consumer interface, ConsumeV2 implements both COOPERATIVE and EAGER rebalance protocol.
	//
	// COOPERATIVE rebalance protocol works as follows:
	// 1. The consumers join the group (as explained in https://kafka.apache.org/documentation/#intro_consumers)
	//    and is assigned their "fair share" of partitions, aka 'claims'.
	// 2. Comparing to the previous assignments, the newly-added partitions and revoked partitions are calculated.
	// 3. For revoked partitions, `ConsumeClaim` loops of these partitions should be exited as quickly as possible.
	//    Then `Cleanup` hook is called to allow the user to perform any final tasks.
	//    Finally, marked offsets are committed one last time before claims are released.
	// 4. For newly-added partitions, `Setup` hook is called to notify the user
	//    of the claims and allow any necessary preparation or alteration of state.
	//    Then several `ConsumeClaim` functions are called in separate goroutines, which is required to be thread-safe.
	// 5. For intersection of the previous and current assignments, nothing happens.
	// 6. If there are revoked partitions, ConsumeV2 will return.
	//    ConsumeV2 will be called again to trigger a new rebalance so that leader can re-assigned the revoked partitions
	//    to other consumers.
	//
	// The difference between COOPERATIVE and EAGER rebalance protocol is that,
	// when a rebalance happens, EAGER rebalance protocol will revoke all the partitions in the current generation,
	// no matter whether they will be assigned to the same consumer later or not.
	//
	// Please note, once ctx is done, `ConsumeClaim` loops must exit as quickly as possible.
	// Otherwise, it will be kicked out of the next generation and cause offset commit failures.
	ConsumeV2(ctx context.Context, topics []string, handler ConsumerGroupHandlerV2) error

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

	// PauseAll suspends fetching from all partitions. Future calls to the broker will not return any
	// records from these partitions until they have been resumed using Resume()/ResumeAll().
	// Note that this method does not affect partition subscription.
	// In particular, it does not cause a group rebalance when automatic assignment is used.
	PauseAll()

	// ResumeAll resumes all partitions which have been paused with Pause()/PauseAll().
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
	generationID    int32
	errors          chan error

	lock       sync.Mutex
	errorsLock sync.RWMutex
	closed     chan none
	closeOnce  sync.Once

	userData []byte

	isLeader                     bool
	protocol                     RebalanceProtocol
	ownedPartitions              map[string][]int32
	offsetManager                *offsetManager
	claims                       map[string]map[int32]*partitionClaim
	claimsLock                   sync.RWMutex
	handlerV2                    ConsumerGroupHandlerV2
	strategy                     BalanceStrategy
	allSubscribedTopicPartitions map[string][]int32
	allSubscribedTopics          []string
	wg                           sync.WaitGroup
	subscribedTopics             []string

	metricRegistry metrics.Registry
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

	// start heartbeat loop
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
		client:          client,
		consumer:        consumer,
		config:          config,
		groupID:         groupID,
		errors:          make(chan error, config.ChannelBufferSize),
		closed:          make(chan none),
		userData:        config.Consumer.Group.Member.UserData,
		ownedPartitions: make(map[string][]int32),
		metricRegistry:  newCleanupRegistry(config.MetricRegistry),
		claims:          make(map[string]map[int32]*partitionClaim),
	}
	if config.Consumer.Group.InstanceId != "" && config.Version.IsAtLeast(V2_3_0_0) {
		cg.groupInstanceId = &config.Consumer.Group.InstanceId
	}

	// select the rebalance protocol such that:
	//   1. only consider protocols that are supported by all BalanceStrategies. If there is no common protocols supported
	//      across all the BalanceStrategies, return an error.
	//   2. if there are multiple protocols that are commonly supported, select the one with the highest value (i.e. the
	//      value indicates how advanced the protocol is)
	var supportedProtocols RebalanceProtocolSlice
	if config.Consumer.Group.Rebalance.Strategy != nil {
		supportedProtocols = config.Consumer.Group.Rebalance.Strategy.SupportedProtocols()
	} else {
		supportedProtocols = config.Consumer.Group.Rebalance.GroupStrategies[0].SupportedProtocols()
		for _, strategy := range config.Consumer.Group.Rebalance.GroupStrategies {
			supportedProtocols = supportedProtocols.retainAll(strategy.SupportedProtocols())
		}
	}
	if len(supportedProtocols) == 0 {
		return nil, ConfigurationError("no common rebalance protocol found")
	}
	sort.Sort(supportedProtocols)
	cg.protocol = supportedProtocols[len(supportedProtocols)-1]

	Logger.Printf("select %s rebalance protocol", cg.protocol)

	cg.offsetManager, err = newOffsetManagerFromClient(cg.groupID, "", GroupGenerationUndefined, client, nil)
	if err != nil {
		return nil, err
	}

	return cg, nil
}

// Errors implements ConsumerGroup.
func (c *consumerGroup) Errors() <-chan error { return c.errors }

// Close implements ConsumerGroup.
func (c *consumerGroup) Close() (err error) {
	c.closeOnce.Do(func() {
		close(c.closed)

		// In cooperative rebalance protocol, we need to revoke all owned partitions before leaving the group.
		c.lock.Lock()
		c.revokedOwnedPartitions()
		c.lock.Unlock()

		// wait for all ConsumeClaim goroutines to exit
		c.wg.Wait()

		if c.offsetManager != nil {
			err = c.offsetManager.Close()
		}

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

	if c.protocol == COOPERATIVE {
		return fmt.Errorf("use ConsumeV2 instead of Consume for cooperative rebalance protocol")
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

	// Wait for session exit signal
	<-sess.ctx.Done()

	// Gracefully release session claims
	return sess.release(true)
}

// todo: check pause & resume logic
func (c *consumerGroup) ConsumeV2(ctx context.Context, topics []string, handlerV2 ConsumerGroupHandlerV2) error {
	// Quick exit when no topics are provided
	if len(topics) == 0 {
		return fmt.Errorf("no topics provided")
	}

	if handlerV2 == nil {
		return fmt.Errorf("nil handler provided")
	}

	// Ensure group is not closed
	select {
	case <-c.closed:
		return ErrClosedConsumerGroup
	default:
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	return c.start(ctx, topics, handlerV2)
}

func (c *consumerGroup) start(ctx context.Context, topics []string, handlerV2 ConsumerGroupHandlerV2) error {
	c.modifySubscribedTopicsAndListener(topics, handlerV2)

	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	// todo: heartbeat should end here
	syncGroupResponse, err := c.joinGroup(ctx, topics, c.config.Consumer.Group.Rebalance.Retry.Max)
	if err != nil {
		c.revokedOwnedPartitions()
		return err
	}

	// start heartbeat once the status is stable
	hbDying, hbDone := c.startHeartbeatLoop(cancelFunc)

	// update the offset manager with new generation, memberID
	c.offsetManager.Update(c.memberID, c.generationID)

	// Retrieve and sort claims
	var claims map[string][]int32
	if len(syncGroupResponse.MemberAssignment) > 0 {
		members, err := syncGroupResponse.GetMemberAssignment()
		if err != nil {
			return err
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

	newAssignedPartitions := diffAssignment(claims, c.ownedPartitions)
	if c.protocol == COOPERATIVE {
		revokedPartitions := diffAssignment(c.ownedPartitions, claims)
		Logger.Printf("updating consumer(group:%s, member:%s, generation:%d, isLeader:%v)\n"+
			"*** All Assignments: %v\n"+
			"*** New Partitions: %v\n"+
			"*** Revoked Partitions: %v\n",
			c.groupID, c.memberID, c.generationID, c.isLeader,
			claims, newAssignedPartitions, revokedPartitions)

		if len(revokedPartitions) > 0 {
			err = c.revokedPartitions(revokedPartitions)
			if err != nil {
				Logger.Printf("error when revoking partitions: %v", err)
			}
			cancelFunc()
		}
	}
	c.ownedPartitions = claims

	err = c.startNewPartitions(newAssignedPartitions)
	if err != nil {
		c.revokedOwnedPartitions()
		cancelFunc()
	}

	// only the leader needs to check whether there are newly-added partitions in order to trigger a rebalance
	if c.isLeader {
		go c.loopCheckPartitionNumbers(ctx, cancelFunc, c.allSubscribedTopicPartitions, c.allSubscribedTopics)
	}

	select {
	case <-c.closed:
		cancelFunc()
	case <-ctx.Done():
	}
	Logger.Printf("consumer(group:%s, member:%s, generation:%d, isLeader:%v) context is done\n",
		c.groupID, c.memberID, c.generationID, c.isLeader)

	// if using EAGER rebalance protocol, we need to revoke all owned partitions before sending new JoinGroupRequest
	if c.protocol == EAGER {
		c.revokedOwnedPartitions()
	}

	// make sure heartbeat loop is stopped
	close(hbDying)
	<-hbDone
	return nil
}

func (c *consumerGroup) joinGroup(ctx context.Context, topics []string, retries int) (*SyncGroupResponse, error) {
	coordinator, err := c.joinPrepare(topics)
	if err != nil {
		return c.retryJoinGroup(ctx, topics, retries-1, err)
	}

	// todo: add metrics

	// Join consumer group
	join, err := c.joinGroupRequest(coordinator, topics)
	if err != nil {
		_ = coordinator.Close()
		return nil, err
	}

	switch join.Err {
	case ErrNoError:
		c.memberID = join.MemberId
	case ErrUnknownMemberId, ErrIllegalGeneration:
		// reset member ID and retry immediately
		c.memberID = ""
		return c.joinGroup(ctx, topics, retries)
	case ErrNotCoordinatorForConsumer, ErrRebalanceInProgress, ErrOffsetsLoadInProgress:
		// retry after backoff
		return c.retryJoinGroup(ctx, topics, retries, join.Err)
	case ErrMemberIdRequired:
		// from JoinGroupRequest v4 onwards (due to KIP-394) if the client starts
		// with an empty member id, it needs to get the assigned id from the
		// response and send another join request with that id to actually join the
		// group
		c.memberID = join.MemberId
		return c.retryJoinGroup(ctx, topics, retries+1 /*keep retry time*/, join.Err)
	case ErrFencedInstancedId:
		if c.groupInstanceId != nil {
			Logger.Printf("JoinGroup failed: group instance id %s has been fenced\n", *c.groupInstanceId)
		}
		return nil, join.Err
	default:
		return nil, join.Err
	}

	c.generationID = join.GenerationId

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
	c.strategy = strategy

	// Prepare distribution plan if we joined as the leader
	var plan BalanceStrategyPlan
	var members map[string]ConsumerGroupMemberMetadata

	if join.LeaderId == join.MemberId {
		members, err = join.GetMembers()
		if err != nil {
			return nil, err
		}

		c.allSubscribedTopicPartitions, c.allSubscribedTopics, plan, err = c.balance(strategy, members)
		if err != nil {
			return nil, err
		}
	}

	// Sync consumer group
	syncGroupResponse, err := c.syncGroupRequest(coordinator, members, plan, join.GenerationId, strategy)
	if err != nil {
		_ = coordinator.Close()
		return nil, err
	}

	switch syncGroupResponse.Err {
	case ErrNoError:
		c.memberID = join.MemberId
		c.generationID = join.GenerationId
	case ErrUnknownMemberId, ErrIllegalGeneration:
		// reset member ID and retry immediately
		c.memberID = ""
		return c.joinGroup(ctx, topics, retries)
	case ErrNotCoordinatorForConsumer, ErrRebalanceInProgress, ErrOffsetsLoadInProgress:
		return c.retryJoinGroup(ctx, topics, retries, syncGroupResponse.Err)
	case ErrFencedInstancedId:
		if c.groupInstanceId != nil {
			Logger.Printf("JoinGroup failed: group instance id %s has been fenced\n", *c.groupInstanceId)
		}
		return nil, syncGroupResponse.Err
	default:
		return nil, syncGroupResponse.Err
	}
	if join.LeaderId == join.MemberId {
		c.isLeader = true
	}

	if c.isLeader && c.protocol == COOPERATIVE && c.strategy.Name() != CooperativeStickyBalanceStrategyName {
		err = validateCooperativeAssignment(members, plan)
		if err != nil {
			return nil, err
		}
	}

	return syncGroupResponse, nil
}

func (c *consumerGroup) retryJoinGroup(ctx context.Context, topics []string, retries int, previousErr error) (*SyncGroupResponse, error) {
	if retries <= 0 {
		return nil, previousErr
	}

	nextRetryTimer := time.NewTimer(c.config.Consumer.Group.Rebalance.Retry.Backoff)
	defer nextRetryTimer.Stop()

	select {
	case <-ctx.Done():
		return nil, previousErr
	case <-nextRetryTimer.C:
		return c.joinGroup(ctx, topics, retries)
	case <-c.closed:
		return nil, ErrClosedConsumerGroup
	}
}

// Used by COOPERATIVE rebalance protocol only.

// Validate the assignments returned by the BalanceStrategy such that no owned partitions are going to
// be reassigned to a different consumer directly: if the BalanceStrategy wants to reassign an owned partition,
// it must first remove it from the new assignment of the current owner so that it is not assigned to any
// member, and then in the next rebalance it can finally reassign those partitions not owned by anyone to consumers.
func validateCooperativeAssignment(previousAssignment map[string]ConsumerGroupMemberMetadata, currentAssignment BalanceStrategyPlan) error {
	set := computePartitionsTransferringOwnership(previousAssignment, currentAssignment)
	if len(set) > 0 {
		var topicPartitions []string
		for k := range set {
			topicPartitions = append(topicPartitions, fmt.Sprintf("%s/%d", k.Topic, k.Partition))
		}
		return fmt.Errorf("in the customized cooperative rebalance strategy, "+
			"topic-partitions %v should be revoked before reassigning them to other consumers", topicPartitions)
	}
	return nil
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
		c.generationID = join.GenerationId
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
		return c.retryNewSession(ctx, topics, handler, retries+1 /*keep retry time*/, false)
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
	session, err := newConsumerGroupSession(ctx, c, claims, handler)
	if err != nil {
		return nil, err
	}

	// only the leader needs to check whether there are newly-added partitions in order to trigger a rebalance
	if join.LeaderId == join.MemberId {
		go c.loopCheckPartitionNumbers(session.ctx, session.cancel, allSubscribedTopicPartitions, allSubscribedTopics)
	}

	return session, err
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
		Version:      2,
		Topics:       topics,
		UserData:     c.userData,
		GenerationID: c.generationID,
	}

	for topic, partitions := range c.ownedPartitions {
		meta.OwnedPartitions = append(meta.OwnedPartitions, &OwnedPartition{Topic: topic, Partitions: partitions})
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
			Err:       err,
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

func (c *consumerGroup) loopCheckPartitionNumbers(ctx context.Context, cancelFunc context.CancelFunc, allSubscribedTopicPartitions map[string][]int32, topics []string) {
	if c.config.Metadata.RefreshFrequency == time.Duration(0) {
		return
	}

	defer cancelFunc()

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
					return // trigger defer cancelFunc()
				}
			}
		}
		select {
		case <-pause.C:
		case <-ctx.Done():
			Logger.Printf(
				"consumergroup/%s loop check partition number goroutine will exit, topics %s\n",
				c.groupID, topics)
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

func (c *consumerGroup) revokedPartitions(revokedPartitions map[string][]int32) error {
	Logger.Printf("consumer(group:%s, member:%s, generation:%d, isLeader:%v) revoking partitions: %v\n",
		c.groupID, c.memberID, c.generationID, c.isLeader, revokedPartitions)

	// close revoked partition consumers
	c.removeClaims(revokedPartitions)

	c.handlerV2.Cleanup(c.offsetManager, revokedPartitions)

	// close partition offset managers for revoked partitions
	if err := c.offsetManager.RemovePartitions(revokedPartitions); err != nil {
		Logger.Printf("error when removing partition offset managers for %v, err: %v", revokedPartitions, err)
		return err
	}
	return nil
}

func (c *consumerGroup) revokedOwnedPartitions() {
	if len(c.ownedPartitions) > 0 {
		err := c.revokedPartitions(c.ownedPartitions)
		if err != nil {
			Logger.Printf("error revoking owned partitions: %v", err)
		}
		c.ownedPartitions = make(map[string][]int32)
	}
}

func (c *consumerGroup) startNewPartitions(newAssignedPartitions map[string][]int32) error {
	Logger.Printf("consumer(group:%s, member:%s, generation:%d, isLeader:%v) starting new assigned partitions: %v",
		c.groupID, c.memberID, c.generationID, c.isLeader, newAssignedPartitions)

	// create partition offset managers for each new assigned partitions
	for topic, partitions := range newAssignedPartitions {
		for _, partition := range partitions {
			pom, err := c.offsetManager.ManagePartition(topic, partition)
			if err != nil {
				Logger.Printf("unable to create partition offset manager for %s/%d, err: %v", topic, partition, err)
				return err
			}

			// handle POM errors
			go func(topic string, partition int32) {
				for err := range pom.Errors() {
					c.handleError(err, topic, partition)
				}
			}(topic, partition)
		}
	}

	c.handlerV2.Setup(c.offsetManager, newAssignedPartitions)

	var errs ConsumerErrors
	var errsLock sync.Mutex

	var wg sync.WaitGroup
	// create partition consumers for each new assigned partitions
	for topic, partitions := range newAssignedPartitions {
		for _, partition := range partitions {
			wg.Add(1)
			// get next offset
			go func(topic string, partition int32) {
				defer wg.Done()

				offset := c.config.Consumer.Offsets.Initial
				if pom := c.offsetManager.findPOM(topic, partition); pom != nil {
					offset, _ = pom.NextOffset()
				}

				claim, err := c.newConsumerGroupClaim(topic, partition, offset)
				if err != nil {
					Logger.Printf("unable to create consumer group claim for %s/%d, err: %v", topic, partition, err)
					errsLock.Lock()
					errs = append(errs, &ConsumerError{
						Topic:     topic,
						Partition: partition,
						Err:       err,
					})
					errsLock.Unlock()
					return
				}
				pc, err := c.addClaim(claim)
				if err != nil {
					Logger.Printf("unable to add consumer group claim for %s/%d, err: %v", topic, partition, err)
					errsLock.Lock()
					errs = append(errs, &ConsumerError{
						Topic:     topic,
						Partition: partition,
						Err:       err,
					})
					errsLock.Unlock()
					return
				}

				// handle errors
				go func(pc *partitionClaim) {
					for err := range pc.claim.Errors() {
						c.handleError(err, pc.claim.topic, pc.claim.partition)
					}
				}(pc)

				c.wg.Add(1)
				go func(pc *partitionClaim) {
					defer c.wg.Done()

					pc.wg.Add(1)
					defer pc.wg.Done()
					c.handlerV2.ConsumeClaim(pc.ctx, c.offsetManager, claim)
				}(pc)
			}(topic, partition)
		}
	}
	wg.Wait()

	if len(errs) > 0 {
		return errs
	}
	return nil
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
	parent       *consumerGroup
	memberID     string
	generationID int32
	handler      ConsumerGroupHandler

	claims  map[string][]int32
	offsets *offsetManager
	ctx     context.Context
	cancel  func()

	waitGroup       sync.WaitGroup
	releaseOnce     sync.Once
	hbDying, hbDead chan none
}

func newConsumerGroupSession(ctx context.Context, parent *consumerGroup, claims map[string][]int32, handler ConsumerGroupHandler) (*consumerGroupSession, error) {
	// init context
	ctx, cancel := context.WithCancel(ctx)

	// init offset manager
	offsets, err := newOffsetManagerFromClient(parent.groupID, parent.memberID, parent.generationID, parent.client, cancel)
	if err != nil {
		return nil, err
	}

	// init session
	sess := &consumerGroupSession{
		parent:       parent,
		memberID:     parent.memberID,
		generationID: parent.generationID,
		handler:      handler,
		offsets:      offsets,
		claims:       claims,
		ctx:          ctx,
		cancel:       cancel,
		hbDying:      make(chan none),
		hbDead:       make(chan none),
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
				defer sess.cancel()

				// consume a single topic/partition, blocking
				sess.consume(topic, partition)
			}(topic, partition)
		}
	}
	return sess, nil
}

func (s *consumerGroupSession) Claims() map[string][]int32 { return s.claims }
func (s *consumerGroupSession) MemberID() string           { return s.memberID }
func (s *consumerGroupSession) GenerationID() int32        { return s.generationID }

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
	claim, err := s.parent.newConsumerGroupClaim(topic, partition, offset)
	if err != nil {
		s.parent.handleError(err, topic, partition)
		return
	}

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
			s.cancel()
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

// ConsumerGroupHandlerV2 instances are used to handle individual topic/partition claims.
// It also provides hooks triggered when adding new partitions or revoking existing ones.
//
// The difference with ConsumerGroupHandler interface is that ConsumerGroupHandlerV2 supports COOPERATIVE rebalancing protocol.
// You should always pass the same ConsumerGroupHandlerV2 instance in ConsumeV2,
// unless you want to change the handler implementation during the lifetime of the consumer group.
//
// PLEASE NOTE that ConsumeClaim is likely be called from several goroutines concurrently,
// ensure that all state is safely protected against race conditions.
type ConsumerGroupHandlerV2 interface {
	// Setup runs at the beginning of setting up new assigned partitions, before ConsumeClaim.
	// For EAGER rebalance strategy, this is to set up all assigned partitions.
	// For COOPERATIVE rebalance strategy, this is only to set up new assigned partitions.
	// Note that even if there are no new assigned partitions, this method will still be called after rebalance.
	Setup(offsetManger OffsetManager, newAssignedPartitions map[string][]int32)

	// Cleanup runs after ConsumeClaim, but before the offsets are committed for the claim.
	// For EAGER rebalance strategy, this is to clean up all assigned partitions.
	// For COOPERATIVE rebalance strategy, this is only to clean up revoked partitions.
	Cleanup(offsetManger OffsetManager, revokedPartitions map[string][]int32)

	// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
	// Once ctx is done, ConsumeClaim should return as soon as possible.
	ConsumeClaim(ctx context.Context, offsetManger OffsetManager, claim ConsumerGroupClaim)
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

func (c *consumerGroup) newConsumerGroupClaim(topic string, partition int32, offset int64) (*consumerGroupClaim, error) {
	pcm, err := c.consumer.ConsumePartition(topic, partition, offset)

	if errors.Is(err, ErrOffsetOutOfRange) && c.config.Consumer.Group.ResetInvalidOffsets {
		offset = c.config.Consumer.Offsets.Initial
		pcm, err = c.consumer.ConsumePartition(topic, partition, offset)
	}
	if err != nil {
		return nil, err
	}

	go func() {
		for err := range pcm.Errors() {
			c.handleError(err, topic, partition)
		}
	}()

	return &consumerGroupClaim{
		topic:             topic,
		partition:         partition,
		offset:            offset,
		PartitionConsumer: pcm,
	}, nil
}

// todo: make it concurrent
func (c *consumerGroup) addClaim(claim *consumerGroupClaim) (*partitionClaim, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	// todo check duplication
	pc := &partitionClaim{
		claim:      claim,
		ctx:        ctx,
		cancelFunc: cancelFunc,
	}

	err := c.setPartitionClaim(pc)
	if err != nil {
		return nil, err
	}
	return pc, nil
}

func (c *consumerGroup) removeClaims(revokedPartitions map[string][]int32) {
	var wg sync.WaitGroup
	revokedPausedPartitions := c.consumer.(*consumer).collectPausedPartitions(revokedPartitions)
	if len(revokedPausedPartitions) > 0 {
		Logger.Printf("The pause flag in partitions %v will be removed due to revocation\n", revokedPausedPartitions)
	}

	for topic, partitions := range revokedPartitions {
		for _, partition := range partitions {
			wg.Add(1)
			go func(topic string, partition int32) {
				defer wg.Done()

				pc := c.getPartitionClaim(topic, partition)
				if pc == nil {
					return
				}

				pc.cancelFunc()
				pc.claim.AsyncClose()

				// wait until ConsumerClaim goroutine returns
				pc.wg.Wait()
				// wait until claim is closed
				for _, err := range pc.claim.waitClosed() {
					c.handleError(err, topic, partition)
				}
			}(topic, partition)
		}
	}
	wg.Wait()

	for topic, partitions := range revokedPartitions {
		for _, partition := range partitions {
			c.removePartitionClaim(topic, partition)
		}
	}
}

func (c *consumerGroup) getPartitionClaim(topic string, partition int32) *partitionClaim {
	c.claimsLock.RLock()
	defer c.claimsLock.RUnlock()

	topicClaims, ok := c.claims[topic]
	if !ok {
		return nil
	}
	pc, ok := topicClaims[partition]
	if !ok {
		return nil
	}
	return pc
}

func (c *consumerGroup) setPartitionClaim(pc *partitionClaim) error {
	c.claimsLock.Lock()
	defer c.claimsLock.Unlock()

	if _, ok := c.claims[pc.claim.topic]; !ok {
		c.claims[pc.claim.topic] = make(map[int32]*partitionClaim)
	}
	if _, ok := c.claims[pc.claim.topic][pc.claim.partition]; ok {
		// safeguard, should never happen
		return fmt.Errorf("partition claim for %s/%d already exists", pc.claim.topic, pc.claim.partition)
	}
	c.claims[pc.claim.topic][pc.claim.partition] = pc
	return nil
}

func (c *consumerGroup) removePartitionClaim(topic string, partition int32) {
	c.claimsLock.Lock()
	defer c.claimsLock.Unlock()

	delete(c.claims[topic], partition)
	if len(c.claims[topic]) == 0 {
		delete(c.claims, topic)
	}
}

func (c *consumerGroup) joinPrepare(topics []string) (*Broker, error) {
	// Refresh metadata for requested topics
	if err := c.client.RefreshMetadata(topics...); err != nil {
		return nil, err
	}

	coordinator, err := c.client.Coordinator(c.groupID)
	if err != nil {
		return nil, err
	}

	return coordinator, nil
}

func (c *consumerGroup) startHeartbeatLoop(cancelFunc context.CancelFunc) (chan none, chan none) {
	hbDone := make(chan none)
	hbDying := make(chan none)
	go func() {
		defer close(hbDone)

		pause := time.NewTicker(c.config.Consumer.Group.Heartbeat.Interval)
		defer pause.Stop()

		retryBackoff := time.NewTimer(c.config.Metadata.Retry.Backoff)
		defer retryBackoff.Stop()

		retries := c.config.Metadata.Retry.Max
		for {
			coordinator, err := c.client.Coordinator(c.groupID)
			if err != nil {
				if retries <= 0 {
					c.handleError(err, "", -1)
					return
				}
				retryBackoff.Reset(c.config.Metadata.Retry.Backoff)
				select {
				case <-hbDying:
					return
				case <-retryBackoff.C:
					retries--
				}
				continue
			}

			resp, err := c.heartbeatRequest(coordinator, c.memberID, c.generationID)
			if err != nil {
				_ = coordinator.Close()

				if retries <= 0 {
					c.handleError(err, "", -1)
					return
				}

				retries--
				continue
			}

			switch resp.Err {
			case ErrNoError:
				retries = c.config.Metadata.Retry.Max
			case ErrRebalanceInProgress:
				retries = c.config.Metadata.Retry.Max
				cancelFunc()
			case ErrUnknownMemberId, ErrIllegalGeneration:
				retries = c.config.Metadata.Retry.Max
				cancelFunc()
			case ErrFencedInstancedId:
				if c.groupInstanceId != nil {
					Logger.Printf("heartbeat failed: group instance id %s has been fenced\n", *c.groupInstanceId)
				}
				c.handleError(resp.Err, "", -1)
				retries = c.config.Metadata.Retry.Max
				cancelFunc()
			default:
				c.handleError(resp.Err, "", -1)
				Logger.Printf("heartbeat failed with unexpected error: %s\n", resp.Err)
				retries = c.config.Metadata.Retry.Max
				cancelFunc()
			}

			select {
			case <-pause.C:
			case <-hbDying:
				return
			}
		}
	}()
	return hbDying, hbDone
}

func (c *consumerGroup) modifySubscribedTopicsAndListener(topics []string, handlerV2 ConsumerGroupHandlerV2) {
	// for the first time
	if c.handlerV2 == nil {
		c.handlerV2 = handlerV2
		c.subscribedTopics = topics
		return
	}

	// if listener is changed, revoke all owned partitions
	if c.handlerV2 != handlerV2 {
		c.revokedOwnedPartitions()
		c.handlerV2 = handlerV2
		c.subscribedTopics = topics
		return
	}

	// if topics are changed, only revoke removed topics
	removedTopics := diffTopicSlice(c.subscribedTopics, topics)

	if len(removedTopics) > 0 {
		removedTopicPartitions := make(map[string][]int32)
		for topic, partitions := range c.ownedPartitions {
			if _, ok := removedTopics[topic]; ok {
				removedTopicPartitions[topic] = partitions
			}
		}
		err := c.revokedPartitions(removedTopicPartitions)
		if err != nil {
			Logger.Printf("error revoking owned removed topic/partitions: %v", err)
		}
	}
	c.subscribedTopics = topics
}

type partitionClaim struct {
	claim      *consumerGroupClaim
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
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

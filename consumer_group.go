package sarama

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ErrClosedConsumerGroup is the error returned when a method is called on a consumer group that has been closed.
var ErrClosedConsumerGroup = errors.New("kafka: tried to use a consumer group that was closed")

// ConsumerGroup is responsible for dividing up processing of topics and partitions
// over a collection of processes (the members of the consumer group).
type ConsumerGroup interface {
	// Consume joins a cluster of consumers for a given list of topics and
	// starts a blocking consumer session through the ConsumerGroupHandler.
	//
	// The session will exit and all its handlers stopped either when:
	// 1. the context is cancelled by the user
	// 2. first handler exits
	// 3. a rebalance cycle is initated server-side
	//
	// Please note that the handler will be applied to each of the claimed partitions
	// in separate goroutines and must therefore be thread-safe. You can only run a single
	// session at a time and must close the previous session before initiating a new one.
	Consume(ctx context.Context, topics []string, handler ConsumerGroupHandler) error

	// Errors returns a read channel of errors that occurred during the consumer life-cycle.
	// By default, errors are logged and not returned over this channel.
	// If you want to implement any custom error handling, set your config's
	// Consumer.Return.Errors setting to true, and read from this channel.
	Errors() <-chan error

	// Close stops the ConsumerGroup and detaches any running sessions. It is required to call
	// this function before the object passes out of scope, as it will otherwise leak memory.
	Close() error
}

type consumerGroup struct {
	client    Client
	ownClient bool

	config   *Config
	consumer Consumer
	groupID  string
	memberID string
	errors   chan error

	session *consumerGroupSession
	lock    sync.Mutex

	closed    chan none
	closeOnce sync.Once
}

// NewConsumerGroup creates a new consumer group the given broker addresses and configuration.
func NewConsumerGroup(addrs []string, groupID string, config *Config) (ConsumerGroup, error) {
	client, err := NewClient(addrs, config)
	if err != nil {
		return nil, err
	}

	c, err := NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	c.(*consumerGroup).ownClient = true
	return c, nil
}

// NewConsumerFromClient creates a new consumer group using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this consumer.
// PLEASE NOTE: consumer groups can only re-use but not share clients.
func NewConsumerGroupFromClient(groupID string, client Client) (ConsumerGroup, error) {
	config := client.Config()
	if !config.Version.IsAtLeast(V0_10_2_0) {
		return nil, ConfigurationError("consumer groups require Version to be >= V0_10_2_0")
	}

	consumer, err := NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}

	return &consumerGroup{
		client:   client,
		consumer: consumer,
		config:   config,
		groupID:  groupID,
		errors:   make(chan error, config.ChannelBufferSize),
		closed:   make(chan none),
	}, nil
}

// Errors implements ConsumerGroup.
func (c *consumerGroup) Errors() <-chan error { return c.errors }

// Close implements ConsumerGroup.
func (c *consumerGroup) Close() (err error) {
	c.closeOnce.Do(func() {
		close(c.closed)

		c.lock.Lock()
		defer c.lock.Unlock()

		// stop session
		if c.session != nil {
			if e := c.session.release(true); e != nil {
				err = e
			}
			c.session = nil
		}

		// leave group
		if e := c.leave(); e != nil {
			err = e
		}

		// drain errors
		go func() {
			close(c.errors)
		}()
		for e := range c.errors {
			err = e
		}

		if c.ownClient {
			if e := c.client.Close(); e != nil {
				err = e
			}
		}
	})
	return
}

// Consume implements ConsumerGroup.
func (c *consumerGroup) Consume(ctx context.Context, topics []string, handler ConsumerGroupHandler) error {
	// Ensure group is not closed
	select {
	case <-c.closed:
	default:
	}

	// Quick exit when no topics are provided
	if len(topics) == 0 {
		return fmt.Errorf("no topics provided")
	}

	// Start session
	sess, err := c.startSession(ctx, topics, handler)
	if err == ErrClosedClient {
		return ErrClosedConsumerGroup
	} else if err != nil {
		return err
	}

	// Wait for session exit signal
	<-sess.ctx.Done()

	// Gracefully release session claims
	err = sess.release(true)
	c.lock.Lock()
	if c.session == sess {
		c.session = nil
	}
	c.lock.Unlock()
	return err
}

func (c *consumerGroup) startSession(ctx context.Context, topics []string, handler ConsumerGroupHandler) (*consumerGroupSession, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.session != nil {
		return nil, fmt.Errorf("another consumer group session already in progress")
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

	sess, err := c.newSession(ctx, coordinator, topics, handler, c.config.Consumer.Group.Rebalance.Retry.Max)
	if err != nil {
		return nil, err
	}

	// Store session
	c.session = sess
	return sess, nil
}

func (c *consumerGroup) newSession(ctx context.Context, coordinator *Broker, topics []string, handler ConsumerGroupHandler, retries int) (*consumerGroupSession, error) {
	select {
	case <-c.closed:
		return nil, ErrClosedConsumerGroup
	default:
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
		return c.newSession(ctx, coordinator, topics, handler, retries)
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
		if retries <= 0 {
			return nil, sync.Err
		}

		select {
		case <-c.closed:
			return nil, ErrClosedConsumerGroup
		case <-time.After(c.config.Consumer.Group.Rebalance.Retry.Backoff):
		}

		return c.newSession(ctx, coordinator, topics, handler, retries-1)
	default:
		return nil, sync.Err
	}

	// Retrieve and sort claims
	var claims map[string][]int32
	if len(sync.MemberAssignment) > 0 {
		members, err := sync.GetMemberAssignment()
		if err != nil {
			return nil, err
		}
		claims = members.Topics

		for _, partitions := range claims {
			sort.Sort(int32Slice(partitions))
		}
	}

	return newConsumerGroupSession(c, ctx, claims, join.MemberId, join.GenerationId, handler)
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

func (c *consumerGroup) heartbeat(memberID string, generationID int32) error {
	coordinator, err := c.client.Coordinator(c.groupID)
	if err != nil {
		return err
	}

	resp, err := coordinator.Heartbeat(&HeartbeatRequest{
		GroupId:      c.groupID,
		MemberId:     memberID,
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

func (c *consumerGroup) handleError(err error, topic string, partition int32) {
	if _, ok := err.(*ConsumerError); !ok && topic != "" && partition > -1 {
		err = &ConsumerError{
			Topic:     topic,
			Partition: partition,
			Err:       err,
		}
	}

	if c.config.Consumer.Return.Errors {
		select {
		case <-c.closed:
		case c.errors <- err:
		}
	} else {
		Logger.Println(err)
	}
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

	waitGroup   sync.WaitGroup
	releaseOnce sync.Once
}

func newConsumerGroupSession(parent *consumerGroup, ctx context.Context, claims map[string][]int32, memberID string, generationID int32, handler ConsumerGroupHandler) (*consumerGroupSession, error) {
	// init offset manager
	offsets, err := newOffsetManagerFromClient(parent.groupID, memberID, generationID, parent.client)
	if err != nil {
		return nil, err
	}

	// init context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)

	// init session
	sess := &consumerGroupSession{
		parent:       parent,
		memberID:     memberID,
		generationID: generationID,
		handler:      handler,
		offsets:      offsets,
		claims:       claims,
		ctx:          ctx,
		cancel:       cancel,
	}

	// start heartbeat loop
	sess.waitGroup.Add(1)
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

	// handle errors
	go func() {
		for err := range claim.Errors() {
			s.parent.handleError(err, topic, partition)
		}
	}()

	// trigger close when session is done
	go func() {
		<-s.ctx.Done()
		claim.AsyncClose()
	}()

	// start processing
	if err := s.handler.ConsumeClaim(s, claim); err != nil {
		s.parent.handleError(err, topic, partition)
	}

	// ensure consumer is clased & drained
	claim.AsyncClose()
	for _, err := range claim.waitClosed() {
		s.parent.handleError(err, topic, partition)
	}
	return
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
				s.parent.handleError(err, "", -1)
				err = e
			}
		}

		if e := s.offsets.Close(); e != nil {
			err = e
		}
	})

	return
}

func (s *consumerGroupSession) heartbeatLoop() {
	defer s.waitGroup.Done()
	defer s.cancel() // trigger the end of the session on exit

	heartbeat := time.NewTicker(s.parent.config.Consumer.Group.Heartbeat.Interval)
	defer heartbeat.Stop()

	for {
		select {
		case <-heartbeat.C:
			if err := s.parent.heartbeat(s.memberID, s.generationID); err != nil {
				switch err {
				case ErrRebalanceInProgress:
				default:
					s.parent.handleError(err, "", -1)
				}
				return
			}
		case <-s.ctx.Done():
			return
		}
	}
}

// --------------------------------------------------------------------

// ConsumerGroupHandler instances are able to handle ConsumerGroupClaim instances.
// PLEASE NOTE that handlers are likely be called from several goroutines concurrently.
// Ensure that all state is safely protected against race conditions.
type ConsumerGroupHandler interface {
	// Setup is run at the beginning of a new session, before Consume.
	Setup(ConsumerGroupSession) error

	// Cleanup is run at the end of a session, before claims are released.
	Cleanup(ConsumerGroupSession) error

	// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
	// Once the Messages() channel is closed, the Handler must finish its
	// processing loop and exit within Config.Consumer.Group.Rebalance.Timeout
	// as the topic/partition may be re-assigned to another group member.
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

	// HighWaterMarkOffset returns the high water mark offset of the partition,
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
	if err == ErrOffsetOutOfRange {
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

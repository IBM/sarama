package sarama

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// TODO : manual vs auto commit mode as in the new offical java client
// currently auto commits

type ConsumerGroup interface {
	// Subscriptions returns the subscripted topics and partitions
	Subscriptions() map[string][]int32

	// Messages returns the read channel for the messages that are returned by
	// the broker.
	Messages() <-chan *ConsumerMessage

	// Errors returns a read channel of errors that occur during offset management, if
	// enabled. By default, errors are logged and not returned over this channel. If
	// you want to implement any custom error handling, set your config's
	// Consumer.Return.Errors setting to true, and read from this channel.
	Errors() <-chan error

	// Notifications returns a channel of Notifications that occur during consumer
	// rebalancing. Notifications will only be emitted over this channel, if your config's
	// Cluster.Return.Notifications setting to true.
	Notifications() <-chan *Notification

	Close() error
}

type consumerGroup struct {
	client     Client
	ownsClient bool

	consumer Consumer

	subscriptions *partitionMap

	consumerID   string
	generationID int32
	group        string
	memberID     string
	topics       []string

	dying, dead chan none

	consuming     int32
	errors        chan error
	messages      chan *ConsumerMessage
	notifications chan *Notification

	commitMu sync.Mutex
}

// initializes a new consumer from an existing client
func NewConsumerGroupFromClient(client Client, group string, topics []string) (ConsumerGroup, error) {
	consumer, err := NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}

	cg := &consumerGroup{
		client:        client,
		consumer:      consumer,
		subscriptions: newPartitionMap(),
		group:         group,
		topics:        topics,
		dying:         make(chan none),
		dead:          make(chan none),
		errors:        make(chan error, client.Config().ChannelBufferSize),
		messages:      make(chan *ConsumerMessage),
		notifications: make(chan *Notification, 1),
	}
	if err := client.RefreshCoordinator(group); err != nil {
		return nil, err
	}

	go cg.mainLoop()
	return cg, nil
}

//  initializes a new consumer
func NewConsumerGroup(addrs []string, group string, topics []string, config *Config) (ConsumerGroup, error) {
	client, err := NewClient(addrs, config)
	if err != nil {
		return nil, err
	}

	cg, err := NewConsumerGroupFromClient(client, group, topics)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	cg.(*consumerGroup).ownsClient = true
	return cg, nil
}

// Messages returns the read channel for the messages that are returned by
// the broker.
func (cg *consumerGroup) Messages() <-chan *ConsumerMessage { return cg.messages }

// Errors returns a read channel of errors that occur during offset management, if
// enabled. By default, errors are logged and not returned over this channel. If
// you want to implement any custom error handling, set your config's
// Consumer.Return.Errors setting to true, and read from this channel.
func (cg *consumerGroup) Errors() <-chan error { return cg.errors }

// Notifications returns a channel of Notifications that occur during consumer
// rebalancing. Notifications will only be emitted over this channel, if your config's
// Cluster.Return.Notifications setting to true.
func (cg *consumerGroup) Notifications() <-chan *Notification { return cg.notifications }

// MarkOffset marks the provided message as processed, alongside a metadata string
// that represents the state of the partition consumer at that point in time. The
// metadata string can be used by another consumer to restore that state, so it
// can resume consumption.
//
// Note: calling MarkOffset does not necessarily commit the offset to the backend
// store immediately for efficiency reasons, and it may never be committed if
// your application crashes. This means that you may end up processing the same
// message twice, and your processing should ideally be idempotent.
func (cg *consumerGroup) MarkOffset(msg *ConsumerMessage, metadata string) {
	cg.subscriptions.Fetch(msg.Topic, msg.Partition).MarkOffset(msg.Offset, metadata)
}

// MarkPartitionOffset marks an offset of the provided topic/partition as processed.
// See MarkOffset for additional explanation.
func (cg *consumerGroup) MarkPartitionOffset(topic string, partition int32, offset int64, metadata string) {
	cg.subscriptions.Fetch(topic, partition).MarkOffset(offset, metadata)
}

// subscriptions returns the consumed topics and partitions
func (cg *consumerGroup) Subscriptions() map[string][]int32 {
	return cg.subscriptions.Info()
}

// CommitOffsets manually commits marked offsets
func (cg *consumerGroup) CommitOffsets() (map[TopicPartition]int64, error) {
	cg.commitMu.Lock()
	defer cg.commitMu.Unlock()

	req := &OffsetCommitRequest{
		Version:                 2,
		ConsumerGroup:           cg.group,
		ConsumerGroupGeneration: cg.generationID,
		ConsumerID:              cg.memberID,
		RetentionTime:           -1,
	}

	var dirty bool
	snap := cg.subscriptions.Snapshot()

	commited := make(map[TopicPartition]int64)

	for tp, state := range snap {
		if state.Dirty {
			req.AddBlock(tp.Topic, tp.Partition, state.Info.Offset, 0, state.Info.Metadata)
			dirty = true
		}
		commited[tp] = state.Info.Offset
	}
	if !dirty {
		return commited, nil
	}

	broker, err := cg.client.Coordinator(cg.group)
	if err != nil {
		return nil, err
	}

	resp, err := broker.CommitOffset(req)
	if err != nil {
		return nil, err
	}

	for topic, perrs := range resp.Errors {
		for partition, kerr := range perrs {
			if kerr != ErrNoError {
				err = kerr
			} else if state, ok := snap[TopicPartition{topic, partition}]; ok {
				cg.subscriptions.Fetch(topic, partition).MarkCommitted(state.Info.Offset)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	return commited, err
}

// Close safely closes the consumer and releases all resources
func (cg *consumerGroup) Close() (err error) {
	close(cg.dying)
	<-cg.dead

	if e := cg.release(); e != nil {
		err = e
	}
	if e := cg.consumer.Close(); e != nil {
		err = e
	}
	close(cg.messages)
	close(cg.errors)

	if e := cg.leaveGroup(); e != nil {
		err = e
	}
	close(cg.notifications)

	if cg.ownsClient {
		if e := cg.client.Close(); e != nil {
			err = e
		}
	}

	return
}

func (cg *consumerGroup) mainLoop() {
	defer close(cg.dead)
	defer atomic.StoreInt32(&cg.consuming, 0)

	for {
		atomic.StoreInt32(&cg.consuming, 0)

		// Remember previous subscriptions
		var notification *Notification
		if cg.client.Config().Group.Return.Notifications {
			notification = newNotification(cg.subscriptions.Info())
		}

		// Rebalance, fetch new subscriptions
		subscriptions, err := cg.rebalance()
		if err != nil {
			cg.rebalanceError(err, notification)
			continue
		}

		// Start the heartbeat
		hbStop, hbDone := make(chan struct{}), make(chan struct{})
		go cg.hbLoop(hbStop, hbDone)

		// subscriptionscribe to topic/partitions
		if err := cg.subscriptionscribe(subscriptions); err != nil {
			close(hbStop)
			<-hbDone
			cg.rebalanceError(err, notification)
			continue
		}

		// Start consuming and comitting offsets
		cmStop, cmDone := make(chan struct{}), make(chan struct{})
		go cg.commitLoop(cmStop, cmDone)
		atomic.StoreInt32(&cg.consuming, 1)

		// Update notification with new claims
		if cg.client.Config().Group.Return.Notifications {
			notification.claim(subscriptions)
			cg.notifications <- notification
		}

		// Wait for signals
		select {
		case <-hbDone:
			close(cmStop)
			<-cmDone
		case <-cmDone:
			close(hbStop)
			<-hbDone
		case <-cg.dying:
			close(cmStop)
			<-cmDone
			close(hbStop)
			<-hbDone
			return
		}
	}
}

// heartbeat loop, triggered by the mainLoop
func (cg *consumerGroup) hbLoop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	ticker := time.NewTicker(cg.client.Config().Group.Heartbeat.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			switch err := cg.heartbeat(); err {
			case nil, ErrNoError:
			case ErrNotCoordinatorForConsumer, ErrRebalanceInProgress:
				return
			default:
				cg.handleError(fmt.Errorf("heartbeat: %v", err))
				return
			}
		case <-stop:
			return
		}
	}
}

// commit loop, triggered by the mainLoop
func (cg *consumerGroup) commitLoop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	ticker := time.NewTicker(cg.client.Config().Consumer.Offsets.CommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := cg.commitOffsetsWithRetry(cg.client.Config().Group.Offsets.Retry.Max); err != nil {
				cg.handleError(fmt.Errorf("commit: %v", err))
				return
			}
		case <-stop:
			return
		}
	}
}

func (cg *consumerGroup) rebalanceError(err error, notification *Notification) {
	if cg.client.Config().Group.Return.Notifications {
		cg.notifications <- notification
	}
	switch err {
	case ErrRebalanceInProgress:
	default:
		cg.handleError(fmt.Errorf("rebalance: %v", err))
	}
	time.Sleep(cg.client.Config().Metadata.Retry.Backoff)
}

func (cg *consumerGroup) handleError(err error) {
	if cg.client.Config().Consumer.Return.Errors {
		select {
		case cg.errors <- err:
		case <-cg.dying:
			return
		}
	} else {
		Logger.Printf("%s error: %v\n", err)
	}
}

// Releases the consumer and commits offsets, called from rebalance() and Close()
func (cg *consumerGroup) release() (err error) {
	// Stop all consumers, don't stop on errors

	if e := cg.subscriptions.Stop(); e != nil {
		err = e
	}

	// Wait for messages to be processed
	time.Sleep(cg.client.Config().Consumer.MaxProcessingTime)

	// Commit offsets
	if e := cg.commitOffsetsWithRetry(cg.client.Config().Group.Offsets.Retry.Max); e != nil {
		err = e
	}

	// Clear subscriptions
	cg.subscriptions.Clear()

	return
}

// --------------------------------------------------------------------

// Performs a heartbeat, part of the mainLoop()
func (cg *consumerGroup) heartbeat() error {
	broker, err := cg.client.Coordinator(cg.group)
	if err != nil {
		return err
	}

	resp, err := broker.Heartbeat(&HeartbeatRequest{
		GroupId:      cg.group,
		MemberId:     cg.memberID,
		GenerationId: cg.generationID,
	})
	if err != nil {
		return err
	}
	return resp.Err
}

// Performs a rebalance, part of the mainLoop()
func (cg *consumerGroup) rebalance() (map[string][]int32, error) {
	Logger.Printf("cluster/consumer memberId=%s rebalance\n", cg.memberID)

	if err := cg.client.RefreshCoordinator(cg.group); err != nil {
		fmt.Println(err)
		return nil, err
	}

	// Release subscriptions
	if err := cg.release(); err != nil {
		fmt.Println(err)
		return nil, err
	}

	// Re-join consumer group
	strategy, err := cg.joinGroup()
	switch {
	case err == ErrUnknownMemberId:
		cg.memberID = ""
		return nil, err
	case err != nil:
		return nil, err
	}
	Logger.Printf("cluster/consumer memberId=%s gen=%d joined group=%s\n", cg.memberID, cg.generationID, cg.group)
	// fmt.Printf("cluster/consumer memberId=%s gen=%d joining group=%s\n", cg.memberID, cg.generationID, cg.group)

	// Sync consumer group state, fetch subscriptions
	subscriptions, err := cg.syncGroup(strategy)
	switch {
	case err == ErrRebalanceInProgress:
		return nil, err
	case err != nil:
		_ = cg.leaveGroup()
		return nil, err
	}
	return subscriptions, nil
}

// Performs the subscriptionscription, part of the mainLoop()
func (cg *consumerGroup) subscriptionscribe(subscriptions map[string][]int32) error {
	// fetch offsets
	offsets, err := cg.fetchOffsets(subscriptions)
	if err != nil {
		_ = cg.leaveGroup()
		return err
	}

	// Create consumers
	for topic, partitions := range subscriptions {
		for _, partition := range partitions {
			if err := cg.createConsumer(topic, partition, offsets[topic][partition]); err != nil {
				_ = cg.release()
				_ = cg.leaveGroup()
				return err
			}
		}
	}
	return nil
}

// --------------------------------------------------------------------

// Send a request to the broker to join group on rebalance()
func (cg *consumerGroup) joinGroup() (*balancer, error) {
	req := &JoinGroupRequest{
		GroupId:        cg.group,
		MemberId:       cg.memberID,
		SessionTimeout: int32(cg.client.Config().Group.Session.Timeout / time.Millisecond),
		ProtocolType:   "consumer",
	}

	meta := &ConsumerGroupMemberMetadata{
		Version: 1,
		Topics:  cg.topics,
	}
	err := req.AddGroupProtocolMetadata(string(StrategyRange), meta)
	if err != nil {
		return nil, err
	}
	err = req.AddGroupProtocolMetadata(string(StrategyRoundRobin), meta)
	if err != nil {
		return nil, err
	}

	broker, err := cg.client.Coordinator(cg.group)
	if err != nil {
		return nil, err
	}

	resp, err := broker.JoinGroup(req)
	if err != nil {
		return nil, err
	} else if resp.Err != ErrNoError {
		return nil, resp.Err
	}

	var strategy *balancer
	if resp.LeaderId == resp.MemberId {
		members, err := resp.GetMembers()
		if err != nil {
			return nil, err
		}
		strategy, err = newBalancerFromMeta(cg.client, members)
		if err != nil {
			return nil, err
		}
	}

	cg.memberID = resp.MemberId
	cg.generationID = resp.GenerationId

	return strategy, nil
}

// Send a request to the broker to sync the group on rebalance().
// Returns a list of topics and partitions to consume.
func (cg *consumerGroup) syncGroup(strategy *balancer) (map[string][]int32, error) {
	req := &SyncGroupRequest{
		GroupId:      cg.group,
		MemberId:     cg.memberID,
		GenerationId: cg.generationID,
	}
	newDistribution := strategy.Perform(cg.client.Config().Group.PartitionStrategy)
	for memberID, topics := range newDistribution {
		if err := req.AddGroupAssignmentMember(memberID, &ConsumerGroupMemberAssignment{
			Version: 1,
			Topics:  topics,
		}); err != nil {
			return nil, err
		}
	}

	broker, err := cg.client.Coordinator(cg.group)
	if err != nil {
		return nil, err
	}

	sync, err := broker.SyncGroup(req)
	if err != nil {
		return nil, err
	} else if sync.Err != ErrNoError {
		return nil, sync.Err
	}

	// Return if there is nothing to subscriptionscribe to
	if len(sync.MemberAssignment) == 0 {
		return nil, nil
	}

	// Get assigned subscriptions
	members, err := sync.GetMemberAssignment()
	if err != nil {
		return nil, err
	}

	// Sort partitions, for each topic
	for topic := range members.Topics {
		sort.Sort(int32Slice(members.Topics[topic]))
	}
	return members.Topics, nil
}

// Fetches latest committed offsets for all subscriptions
func (cg *consumerGroup) fetchOffsets(subscriptions map[string][]int32) (map[string]map[int32]offsetInfo, error) {
	offsets := make(map[string]map[int32]offsetInfo, len(subscriptions))
	req := &OffsetFetchRequest{
		Version:       1,
		ConsumerGroup: cg.group,
	}

	for topic, partitions := range subscriptions {
		offsets[topic] = make(map[int32]offsetInfo, len(partitions))
		for _, partition := range partitions {
			offsets[topic][partition] = offsetInfo{Offset: -1}
			req.AddPartition(topic, partition)
		}
	}

	// Wait for other cluster consumers to process, release and commit
	time.Sleep(cg.client.Config().Consumer.MaxProcessingTime * 2)

	broker, err := cg.client.Coordinator(cg.group)
	if err != nil {
		return nil, err
	}

	resp, err := broker.FetchOffset(req)
	if err != nil {
		return nil, err
	}

	for topic, partitions := range subscriptions {
		for _, partition := range partitions {
			block := resp.GetBlock(topic, partition)
			if block == nil {
				return nil, ErrIncompleteResponse
			}

			if block.Err == ErrNoError {
				offsets[topic][partition] = offsetInfo{Offset: block.Offset, Metadata: block.Metadata}
			} else {
				return nil, block.Err
			}
		}
	}
	return offsets, nil
}

// Send a request to the broker to leave the group on failes rebalance() and on Close()
func (cg *consumerGroup) leaveGroup() error {
	broker, err := cg.client.Coordinator(cg.group)
	if err != nil {
		return err
	}

	_, err = broker.LeaveGroup(&LeaveGroupRequest{
		GroupId:  cg.group,
		MemberId: cg.memberID,
	})
	return err
}

// --------------------------------------------------------------------

func (cg *consumerGroup) createConsumer(topic string, partition int32, info offsetInfo) error {
	Logger.Printf("cluster/consumer %s consume %s/%d from %d\n", cg.memberID, topic, partition, info.NextOffset(cg.client.Config().Consumer.Offsets.Initial))
	// fmt.Printf("cluster/consumer %s consume %s/%d from %d\n", cg.memberID, topic, partition, info.NextOffset(cg.client.Config().Consumer.Offsets.Initial))

	// create forwarder
	pc, err := newPartitionForwarder(cg.consumer, topic, partition, info, cg.client.Config().Consumer.Offsets.Initial)
	if err != nil {
		return nil
	}

	// Store in subscriptions
	cg.subscriptions.Store(topic, partition, pc)

	// Start partition consumer goroutine
	go pc.Loop(cg.messages, cg.errors)

	return nil
}

func (cg *consumerGroup) commitOffsetsWithRetry(retries int) error {
	_, err := cg.CommitOffsets()
	if err != nil && retries > 0 && cg.subscriptions.HasDirty() {
		_ = cg.client.RefreshCoordinator(cg.group)
		return cg.commitOffsetsWithRetry(retries - 1)
	}
	return err
}

// Strategy for partition to consumer assignement
type Strategy string

const (
	// StrategyRange is the default and assigns partition ranges to consumers.
	// Example with six partitions and two consumers:
	//   C1: [0, 1, 2]
	//   C2: [3, 4, 5]
	StrategyRange Strategy = "range"

	// StrategyRoundRobin assigns partitions by alternating over consumers.
	// Example with six partitions and two consumers:
	//   C1: [0, 2, 4]
	//   C2: [1, 3, 5]
	StrategyRoundRobin Strategy = "roundrobin"
)

type TopicPartition struct {
	Topic     string
	Partition int32
}

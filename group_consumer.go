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
	// Subscriptions() map[string][]int32

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

	MarkOffset(msg *ConsumerMessage, metadata string)

	Close() error
}

type consumerGroup struct {
	client     Client
	ownsClient bool

	consumer Consumer

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

	om      OffsetManager
	managed map[TopicPartition]*managedPartition
}

type managedPartition struct {
	pom PartitionOffsetManager
	pc  PartitionConsumer
	fwd *forwarder
}

// initializes a new consumer from an existing client
func NewConsumerGroupFromClient(client Client, group string, topics []string) (ConsumerGroup, error) {
	consumer, err := NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}

	om, err := NewOffsetManagerFromClient(group, client)
	if err != nil {
		return nil, err
	}

	cg := &consumerGroup{
		client:        client,
		om:            om,
		consumer:      consumer,
		group:         group,
		topics:        topics,
		dying:         make(chan none),
		dead:          make(chan none),
		errors:        make(chan error, client.Config().ChannelBufferSize),
		messages:      make(chan *ConsumerMessage),
		notifications: make(chan *Notification, 1),
		managed:       make(map[TopicPartition]*managedPartition),
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

	if mp := cg.managed[TopicPartition{msg.Topic, msg.Partition}]; mp != nil {
		mp.pom.MarkOffset(msg.Offset, metadata)
		// pom.MarkOffset(msg.Offset, metadata)
	}

	// cg.subscriptions.Fetch(msg.Topic, msg.Partition).MarkOffset(msg.Offset, metadata)
}

func (cg *consumerGroup) log(f string, a ...interface{}) {
	s := f
	if len(a) > 0 {
		s = fmt.Sprintf(f, a...)
	}
	fmt.Printf("%s: %s\n", cg.memberID, s)
}

// Close safely closes the consumer and releases all resources
func (cg *consumerGroup) Close() (err error) {
	close(cg.dying)
	cg.log("close dying wait for dead")
	<-cg.dead

	cg.log("cg.release")
	if e := cg.release(); e != nil {
		err = e
	}
	cg.log("cg.consumer.Close")

	if e := cg.consumer.Close(); e != nil {
		err = e
	}
	cg.log("cg close msg/errs")
	close(cg.messages)
	close(cg.errors)

	cg.log("cg.leaveGroup")
	if e := cg.leaveGroup(); e != nil {
		err = e
	}
	close(cg.notifications)

	if cg.ownsClient {
		if e := cg.client.Close(); e != nil {
			err = e
		}
	}
	cg.log("dead")
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
			// notification = newNotification(cg.subscriptions.Info())
		}

		// Rebalance, fetch new subscriptions
		cg.log("trigger rebalance from mainLoop")
		subscriptions, err := cg.rebalance()
		if err != nil {
			cg.rebalanceError(err, notification)
			continue
		}

		// Start the heartbeat
		hbStop, hbDone := make(chan struct{}), make(chan struct{})
		go cg.heartbeatLoop(hbStop, hbDone)

		// subcribe to topic/partitions
		if err := cg.subcribe(subscriptions); err != nil {
			close(hbStop)
			<-hbDone
			cg.rebalanceError(err, notification)
			continue
		}

		// Start consuming and comitting offsets
		// cmStop, cmDone := make(chan struct{}), make(chan struct{})
		// go cg.commitLoop(cmStop, cmDone)
		atomic.StoreInt32(&cg.consuming, 1)

		// Update notification with new claims
		if cg.client.Config().Group.Return.Notifications {
			notification.claim(subscriptions)
			cg.notifications <- notification
		}

		// Wait for signals
		select {
		case <-hbDone:
		case <-cg.dying:
			close(hbStop)
			<-hbDone
			return
		}
	}
}

// heartbeat loop, triggered by the mainLoop
func (cg *consumerGroup) heartbeatLoop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	ticker := time.NewTicker(cg.client.Config().Group.Heartbeat.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			switch err := cg.heartbeat(); err {
			case nil, ErrNoError:
			case ErrNotCoordinatorForConsumer, ErrRebalanceInProgress:
				cg.log("heartbeat broken: %v", err)
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
		cg.log("%v", err)
	}
}

// Releases the consumer and commits offsets, called from rebalance() and Close()
func (cg *consumerGroup) release() (err error) {
	if len(cg.managed) > 0 {
		cg.log("release subscriptions n=%d", len(cg.managed))
	}

	// Stop all consumers, don't stop on errors
	// stop consuming
	for _, mp := range cg.managed {
		// todo add error handling
		mp.fwd.Close()
		mp.pc.Close()
	}

	// Wait for messages to be processed
	time.Sleep(cg.client.Config().Consumer.MaxProcessingTime)

	// Commit offsets
	cg.om.Commit()

	// stop managing these offsets
	for _, mp := range cg.managed {
		// todo add error handling
		mp.pom.Close()
	}

	// clear all by recreating the map
	cg.managed = make(map[TopicPartition]*managedPartition)

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
	// Logger.Printf("cluster/consumer memberId=%s rebalance\n", cg.memberID)
	// cg.log("trigger rebalance")

	if err := cg.client.RefreshCoordinator(cg.group); err != nil {
		cg.log("error on rebalance refresh coord: %v", err)
		return nil, err
	}

	// Release subscriptions
	if err := cg.release(); err != nil {
		cg.log("error on rebalance release: %v", err)
		return nil, err
	}

	// Re-join consumer group
	strategy, err := cg.joinGroup()
	if err != nil {
		cg.log("error on rebalance joinGroup: %v", err)
	}
	switch {
	case err == ErrUnknownMemberId:
		cg.memberID = ""
		return nil, err
	case err != nil:
		return nil, err
	}

	// Logger.Printf("consumerGroup memberId=%s gen=%d joined group=%s\n", cg.memberID, cg.generationID, cg.group)
	cg.log("joining group=%s gen=%d\n", cg.group, cg.generationID)

	if strategy != nil {
		cg.log("I AM MASTER")
	}
	// Sync consumer group state, fetch subscriptions
	subscriptions, err := cg.syncGroup(strategy)
	switch {
	case err == ErrRebalanceInProgress:
		return nil, err
	case err != nil:
		_ = cg.leaveGroup()
		return nil, err
	}
	cg.log("subscribe %v", subscriptions)
	// fmt.Printf("consumerGroup memberId=%s gen=%d group=%s subscriptions=%v\n", cg.memberID, cg.generationID, cg.group, subscriptions)
	return subscriptions, nil
}

// Performs the subscriptionscription, part of the mainLoop()
func (cg *consumerGroup) subcribe(subscriptions map[string][]int32) error {
	// // fetch offsets
	// offsets, err := cg.fetchOffsets(subscriptions)
	// if err != nil {
	// 	_ = cg.leaveGroup()
	// 	return err
	// }

	// Create consumers
	for topic, partitions := range subscriptions {
		for _, partition := range partitions {
			if err := cg.createConsumer(topic, partition); err != nil {
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

	cg.log("join group as %s", cg.memberID)
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
	// TODO : change - annoying to call stuff with empty this pointer (make explicit that we only send an empy
	// request if we are not the leader)
	newDistribution := strategy.Perform(cg.client.Config().Group.PartitionStrategy)
	for memberID, topics := range newDistribution {
		cg.log("  propose memberID=%s topics=%v", memberID, topics)
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
	cg.log("sync group request gen=%d", cg.generationID)
	sync, err := broker.SyncGroup(req)
	if err != nil {
		return nil, err
	} else if sync.Err != ErrNoError {
		return nil, sync.Err
	}

	// Return if there is nothing to subcribe to
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

// TODO : move to sep file later
type forwarder struct {
	msgs   <-chan *ConsumerMessage
	errors <-chan *ConsumerError

	closed      bool
	dying, dead chan none
}

func newForwarder(msgs <-chan *ConsumerMessage, errors <-chan *ConsumerError) *forwarder {
	return &forwarder{
		msgs:   msgs,
		errors: errors,
		dying:  make(chan none),
		dead:   make(chan none),
	}
}

func (f *forwarder) forwardTo(messages chan<- *ConsumerMessage, errors chan<- error) {
	for {
		select {
		case msg := <-f.msgs:
			if msg != nil {
				select {
				case messages <- msg:
				case <-f.dying:
					close(f.dead)
					return
				}
			}
		case err := <-f.errors:
			if err != nil {
				select {
				case errors <- err:
				case <-f.dying:
					close(f.dead)
					return
				}
			}
		case <-f.dying:
			close(f.dead)
			return
		}
	}
}

func (f *forwarder) Close() {
	if f.closed {
		return
	}
	f.closed = true
	close(f.dying)
	<-f.dead
}

func (cg *consumerGroup) createConsumer(topic string, partition int32) error {
	// Logger.Printf("cluster/consumer %s consume %s/%d from %d\n", cg.memberID, topic, partition, 0)

	pom, err := cg.om.ManagePartition(topic, partition)

	if err != nil {
		return err
	}

	offset, _ := pom.NextOffset()

	pc, err := cg.consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		return err
	}

	fwder := newForwarder(pc.Messages(), pc.Errors())

	go fwder.forwardTo(cg.messages, cg.errors)

	cg.managed[TopicPartition{topic, partition}] = &managedPartition{
		fwd: fwder,
		pc:  pc,
		pom: pom,
	}

	return nil

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

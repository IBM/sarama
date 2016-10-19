package sarama

import (
	"fmt"
	"sort"
	"time"
)

// TODO : manual vs auto commit mode as in the new offical java client
// currently auto commits marked offsets

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

	// mark a consumer mesasges as ready to commit
	MarkMessage(msg *ConsumerMessage, metadata string)

	// mark a topic/partition/offset combination as ready to commit
	MarkOffset(topic string, partition int32, offset int64, metadata string)

	// commit marked offsets - only needed if auto commit is disabeled
	Commit()

	// Shutdown the consumer group
	Close() error
}

type consumerGroup struct {
	client     Client
	ownsClient bool

	consumer Consumer
	om       OffsetManager
	managed  map[TopicPartition]*managedPartition

	topics       []string
	group        string
	consumerID   string
	memberID     string
	generationID int32

	dying, dead chan none
	closed      bool

	messages      chan *ConsumerMessage
	notifications chan *Notification
	errors        chan error
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
		errors:        make(chan error),
		messages:      make(chan *ConsumerMessage, client.Config().ChannelBufferSize),
		notifications: make(chan *Notification, 1),
		managed:       make(map[TopicPartition]*managedPartition),
	}

	// we could make this nicer as it break encapsulation
	if com, castOk := om.(*offsetManager); castOk {
		com.cg = cg
	}

	if err := client.RefreshCoordinator(group); err != nil {
		return nil, err
	}

	go cg.mainLoop()
	return cg, nil
}

// initializes a new consumer
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

// MarkMessage marks the provided message as processed, alongside a metadata string
// that represents the state of the partition consumer at that point in time. The
// metadata string can be used by another consumer to restore that state, so it
// can resume consumption.
//
// Note: calling MarMessage does not necessarily commit the offset to the backend
// store immediately for efficiency reasons, and it may never be committed if
// your application crashes. This means that you may end up processing the same
// message twice, and your processing should ideally be idempotent.
func (cg *consumerGroup) MarkMessage(msg *ConsumerMessage, metadata string) {
	cg.MarkOffset(msg.Topic, msg.Partition, msg.Offset, metadata)
}

func (cg *consumerGroup) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	if mp := cg.managed[TopicPartition{topic, partition}]; mp != nil {
		mp.pom.MarkOffset(offset+1, metadata)
	}
}

// commits marked offsets
func (cg *consumerGroup) Commit() {
	cg.om.Commit()
}

func (cg *consumerGroup) log(f string, a ...interface{}) {
	s := f
	if len(a) > 0 {
		s = fmt.Sprintf(f, a...)
	}
	member := cg.memberID
	if member == "" {
		member = "<???>"
	}
	Logger.Printf("consumerGroup/%s/%s %s\n", cg.group, member, s)
}

// Close safely closes the consumer and releases all resources
func (cg *consumerGroup) Close() (err error) {
	if cg.closed {
		return
	}
	cg.log("closing")

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
	cg.log("closed")
	cg.closed = true
	return
}

func (cg *consumerGroup) createConsumer(topic string, partition int32) error {
	cg.log("consume %s/%d", topic, partition)

	pom, err := cg.om.ManagePartition(topic, partition)

	if err != nil {
		return err
	}

	offset, _ := pom.NextOffset()

	oldest, err := cg.client.GetOffset(topic, partition, OffsetOldest)
	if err != nil {
		return err
	}

	newest, err := cg.client.GetOffset(topic, partition, OffsetNewest)
	if err != nil {
		return err
	}

	// well this should not really happen ... the offset should be between oldest and newest
	// unless it's negative for OffsetOldest/OffsetNewest
	if offset >= 0 && offset < oldest {
		offset = oldest
	}

	cg.log("consume %s/%d pom-offset=%d oldest=%d newest=%d", topic, partition, offset, oldest, newest)

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

func (cg *consumerGroup) mainLoop() {
	defer close(cg.dead)

	for {
		var notification *Notification
		if cg.client.Config().Group.Return.Notifications {
			m := make(map[string][]int32)
			for tp := range cg.managed {
				m[tp.Topic] = append(m[tp.Topic], tp.Partition)
			}
			notification = newNotification(m)
		}

		subscriptions, err := cg.rebalance()
		if err != nil {
			cg.rebalanceError(err, notification)
			continue
		}

		// start heartbeat
		hbStop, hbDone := make(chan struct{}), make(chan struct{})
		go cg.heartbeatLoop(hbStop, hbDone)

		// subcribe to topic/partitions
		if err := cg.subcribe(subscriptions); err != nil {
			close(hbStop)
			<-hbDone
			cg.rebalanceError(err, notification)
			continue
		}

		// send notifications
		if cg.client.Config().Group.Return.Notifications {
			notification.claim(subscriptions)
			cg.notifications <- notification
		}

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
				cg.handleError(fmt.Errorf("heartbeat broken: %v", err))
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
		cg.handleError(fmt.Errorf("rebalance error: %v", err))
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
		cg.log("%v", err)
	}
}

// Releases the consumer and commits offsets, called from rebalance() and Close()
func (cg *consumerGroup) release() (err error) {
	if len(cg.managed) > 0 {
		cg.log("release subscriptions n=%d", len(cg.managed))
	} else {
		return
	}

	// stop all consumers, don't stop on errors
	for _, mp := range cg.managed {
		mp.fwd.Close()
		mp.pc.Close()
	}

	// Commit offsets
	cg.om.Commit()

	// stop managing these offsets
	for _, mp := range cg.managed {
		mp.pom.Close()
	}

	// clear all by recreating the map
	cg.managed = make(map[TopicPartition]*managedPartition)

	return
}

func (cg *consumerGroup) heartbeat() error {
	broker, err := cg.client.Coordinator(cg.group)
	if err != nil {
		cg.closeCoordinator(broker, err)
		return err
	}
	resp, err := broker.Heartbeat(&HeartbeatRequest{
		GroupId:      cg.group,
		MemberId:     cg.memberID,
		GenerationId: cg.generationID,
	})
	if err != nil {
		cg.closeCoordinator(broker, err)
		return err
	}
	return resp.Err
}

func (cg *consumerGroup) redistributePartitions(joinResponse *JoinGroupResponse, syncRequest *SyncGroupRequest) error {
	var strategy *balancer

	members, err := joinResponse.GetMembers()

	// fmt.Println("LEADER got group meta data")
	// for m, meta := range members {
	// 	fmt.Printf("  %s: v=%d %v\n", m, meta.Version, meta.Topics)
	// }

	if err != nil {
		return err
	}

	strategy, err = newBalancerFromMeta(cg.client, members)

	if err != nil {
		return err
	}

	newDistribution := strategy.Perform(cg.client.Config().Group.PartitionStrategy)
	for memberID, topics := range newDistribution {
		// cg.log("propose memberID=%s topics=%v", memberID, topics)
		if err := syncRequest.AddGroupAssignmentMember(memberID, &ConsumerGroupMemberAssignment{
			Version: 1,
			Topics:  topics,
		}); err != nil {
			return err
		}
	}
	return nil
}

// Performs a rebalance, part of the mainLoop()
func (cg *consumerGroup) rebalance() (map[string][]int32, error) {
	cg.log("rebalance")

	if err := cg.client.RefreshCoordinator(cg.group); err != nil {
		cg.log("rebalance coordinator error: %v", err)
		return nil, err
	}

	// Release subscriptions
	if err := cg.release(); err != nil {
		cg.log("rebalance group release error: %v", err)
		return nil, err
	}

	// Re-join consumer group
	joinResponse, err := cg.joinGroup()
	if err != nil {
		cg.log("rebalance join group error: %v", err)
	}
	switch {
	case err == ErrUnknownMemberId:
		cg.memberID = ""
		return nil, err
	case err != nil:
		return nil, err
	}

	cg.log("joining group=%s generation=%d\n", cg.group, cg.generationID)

	syncRequest := cg.newSyncGroupRequest()

	if joinResponse.IsLeader() {
		cg.log("elected as leader")
		err := cg.redistributePartitions(joinResponse, syncRequest)
		if err != nil {
			return nil, err
		}
	}

	subscriptions, err := cg.syncGroup(syncRequest)
	switch {
	case err == ErrRebalanceInProgress:
		return nil, err
	case err != nil:
		_ = cg.leaveGroup()
		return nil, err
	}
	cg.log("subscribe %v", subscriptions)
	return subscriptions, nil
}

// Performs the subscriptionscription, part of the mainLoop()
func (cg *consumerGroup) subcribe(subscriptions map[string][]int32) error {
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

// Send a request to the broker to join group on rebalance()
func (cg *consumerGroup) joinGroup() (*JoinGroupResponse, error) {
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
		cg.closeCoordinator(broker, err)
		return nil, err
	}

	response, err := broker.JoinGroup(req)

	if err != nil {
		cg.closeCoordinator(broker, err)
		return nil, err
	} else if response.Err != ErrNoError {
		cg.closeCoordinator(broker, response.Err)
		return nil, response.Err
	}

	cg.log("join group")

	cg.memberID = response.MemberId
	cg.generationID = response.GenerationId

	return response, nil
}

func (cg *consumerGroup) newSyncGroupRequest() *SyncGroupRequest {
	return &SyncGroupRequest{
		GroupId:      cg.group,
		MemberId:     cg.memberID,
		GenerationId: cg.generationID,
	}
}

// Send a request to the broker to sync the group on rebalance().
// Returns a list of topics and partitions to consume.
func (cg *consumerGroup) syncGroup(req *SyncGroupRequest) (map[string][]int32, error) {
	broker, err := cg.client.Coordinator(cg.group)
	if err != nil {
		cg.closeCoordinator(broker, err)
		return nil, err
	}
	cg.log("sync group request generation=%d", cg.generationID)
	sync, err := broker.SyncGroup(req)
	if err != nil {
		cg.closeCoordinator(broker, err)
		return nil, err
	} else if sync.Err != ErrNoError {
		cg.closeCoordinator(broker, sync.Err)
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
		cg.closeCoordinator(broker, err)
		return err
	}
	cg.log("leave group")

	_, err = broker.LeaveGroup(&LeaveGroupRequest{
		GroupId:  cg.group,
		MemberId: cg.memberID,
	})
	if err != nil {
		cg.closeCoordinator(broker, err)
	}
	return err
}

func (cg *consumerGroup) closeCoordinator(broker *Broker, err error) {
	if broker != nil {
		_ = broker.Close()
	}
	switch err {
	case ErrConsumerCoordinatorNotAvailable, ErrNotCoordinatorForConsumer:
		_ = cg.client.RefreshCoordinator(cg.group)
	}
}

// forwards messages and error from source to destination channels
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
		default:
			time.Sleep(20 * time.Millisecond)
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

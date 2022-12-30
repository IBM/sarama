package sarama

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type handler struct {
	*testing.T
	cancel context.CancelFunc
}

func (h *handler) Setup(s ConsumerGroupSession) error   { return nil }
func (h *handler) Cleanup(s ConsumerGroupSession) error { return nil }
func (h *handler) ConsumeClaim(sess ConsumerGroupSession, claim ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		sess.MarkMessage(msg, "")
		h.Logf("consumed msg %v", msg)
		h.cancel()
		break
	}
	return nil
}

// TestConsumerGroupNewSessionDuringOffsetLoad ensures that the consumer group
// will retry Join and Sync group operations, if it receives a temporary
// OffsetsLoadInProgress error response, in the same way as it would for a
// RebalanceInProgress.
func TestConsumerGroupNewSessionDuringOffsetLoad(t *testing.T) {
	config := NewTestConfig()
	config.ClientID = t.Name()
	config.Version = V2_0_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.Retry.Max = 2
	config.Consumer.Offsets.AutoCommit.Enable = false

	broker0 := NewMockBroker(t, 0)

	broker0.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("my-topic", 0, broker0.BrokerID()),
		"OffsetRequest": NewMockOffsetResponse(t).
			SetOffset("my-topic", 0, OffsetOldest, 0).
			SetOffset("my-topic", 0, OffsetNewest, 1),
		"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).
			SetCoordinator(CoordinatorGroup, "my-group", broker0),
		"HeartbeatRequest": NewMockHeartbeatResponse(t),
		"JoinGroupRequest": NewMockSequence(
			NewMockJoinGroupResponse(t).SetError(ErrOffsetsLoadInProgress),
			NewMockJoinGroupResponse(t).SetGroupProtocol(RangeBalanceStrategyName),
		),
		"SyncGroupRequest": NewMockSequence(
			NewMockSyncGroupResponse(t).SetError(ErrOffsetsLoadInProgress),
			NewMockSyncGroupResponse(t).SetMemberAssignment(
				&ConsumerGroupMemberAssignment{
					Version: 0,
					Topics: map[string][]int32{
						"my-topic": {0},
					},
				}),
		),
		"OffsetFetchRequest": NewMockOffsetFetchResponse(t).SetOffset(
			"my-group", "my-topic", 0, 0, "", ErrNoError,
		).SetError(ErrNoError),
		"FetchRequest": NewMockSequence(
			NewMockFetchResponse(t, 1).
				SetMessage("my-topic", 0, 0, StringEncoder("foo")).
				SetMessage("my-topic", 0, 1, StringEncoder("bar")),
			NewMockFetchResponse(t, 1),
		),
	})

	group, err := NewConsumerGroup([]string{broker0.Addr()}, "my-group", config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = group.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	h := &handler{t, cancel}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		topics := []string{"my-topic"}
		if err := group.Consume(ctx, topics, h); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()
	wg.Wait()
}

func TestConsume_RaceTest(t *testing.T) {
	const groupID = "test-group"
	const topic = "test-topic"
	const offsetStart = int64(1234)

	cfg := NewConfig()
	cfg.Version = V2_8_1_0
	cfg.Consumer.Return.Errors = true

	seedBroker := NewMockBroker(t, 1)

	joinGroupResponse := &JoinGroupResponse{}

	syncGroupResponse := &SyncGroupResponse{
		Version: 3, // sarama > 2.3.0.0 uses version 3
	}
	// Leverage mock response to get the MemberAssignment bytes
	mockSyncGroupResponse := NewMockSyncGroupResponse(t).SetMemberAssignment(&ConsumerGroupMemberAssignment{
		Version:  1,
		Topics:   map[string][]int32{topic: {0}}, // map "test-topic" to partition 0
		UserData: []byte{0x01},
	})
	syncGroupResponse.MemberAssignment = mockSyncGroupResponse.MemberAssignment

	heartbeatResponse := &HeartbeatResponse{
		Err: ErrNoError,
	}
	offsetFetchResponse := &OffsetFetchResponse{
		Version:        1,
		ThrottleTimeMs: 0,
		Err:            ErrNoError,
	}
	offsetFetchResponse.AddBlock(topic, 0, &OffsetFetchResponseBlock{
		Offset:      offsetStart,
		LeaderEpoch: 0,
		Metadata:    "",
		Err:         ErrNoError})

	offsetResponse := &OffsetResponse{
		Version: 1,
	}
	offsetResponse.AddTopicPartition(topic, 0, offsetStart)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadataResponse.AddTopic("mismatched-topic", ErrUnknownTopicOrPartition)

	handlerMap := map[string]MockResponse{
		"ApiVersionsRequest": NewMockApiVersionsResponse(t),
		"MetadataRequest":    NewMockSequence(metadataResponse),
		"OffsetRequest":      NewMockSequence(offsetResponse),
		"OffsetFetchRequest": NewMockSequence(offsetFetchResponse),
		"FindCoordinatorRequest": NewMockSequence(NewMockFindCoordinatorResponse(t).
			SetCoordinator(CoordinatorGroup, groupID, seedBroker)),
		"JoinGroupRequest": NewMockSequence(joinGroupResponse),
		"SyncGroupRequest": NewMockSequence(syncGroupResponse),
		"HeartbeatRequest": NewMockSequence(heartbeatResponse),
	}
	seedBroker.SetHandlerByMap(handlerMap)

	cancelCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(4*time.Second))

	defer seedBroker.Close()

	retryWait := 20 * time.Millisecond
	var err error
	clientRetries := 0
outerFor:
	for {
		_, err = NewConsumerGroup([]string{seedBroker.Addr()}, groupID, cfg)
		if err == nil {
			break
		}

		if retryWait < time.Minute {
			retryWait *= 2
		}

		clientRetries++

		timer := time.NewTimer(retryWait)
		select {
		case <-cancelCtx.Done():
			err = cancelCtx.Err()
			timer.Stop()
			break outerFor
		case <-timer.C:
		}
		timer.Stop()
	}
	if err == nil {
		t.Fatalf("should not proceed to Consume")
	}

	if clientRetries <= 0 {
		t.Errorf("clientRetries = %v; want > 0", clientRetries)
	}

	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}

	cancel()
}

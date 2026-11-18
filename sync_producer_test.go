//go:build !functional

package sarama

import (
	"context"
	"errors"
	"log"
	"sync"
	"testing"
)

func TestSyncProducer(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	for range 10 {
		leader.Returns(prodSuccess)
	}

	config := NewTestConfig()
	config.Producer.Return.Successes = true
	producer, err := NewSyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for range 10 {
		msg := &ProducerMessage{
			Topic:    "my_topic",
			Value:    StringEncoder(TestMessage),
			Metadata: "test",
		}

		partition, offset, err := producer.SendMessage(msg)

		if partition != 0 || msg.Partition != partition {
			t.Error("Unexpected partition")
		}
		if offset != 0 || msg.Offset != offset {
			t.Error("Unexpected offset")
		}
		if str, ok := msg.Metadata.(string); !ok || str != "test" {
			t.Error("Unexpected metadata")
		}
		if err != nil {
			t.Error(err)
		}
	}

	safeClose(t, producer)
	leader.Close()
	seedBroker.Close()
}

func TestSyncProducerTransactional(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()
	leader := NewMockBroker(t, 2)
	defer leader.Close()

	config := NewTestConfig()
	config.Version = V0_11_0_0
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Transaction.ID = "test"
	config.Producer.Idempotent = true
	config.Producer.Retry.Max = 5
	config.Net.MaxOpenRequests = 1

	metadataResponse := new(MetadataResponse)
	metadataResponse.Version = 4
	metadataResponse.ControllerID = leader.BrokerID()
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopic("my_topic", ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	client, err := NewClient([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, client)

	findCoordinatorResponse := new(FindCoordinatorResponse)
	findCoordinatorResponse.Coordinator = client.Brokers()[0]
	findCoordinatorResponse.Version = 1
	leader.Returns(findCoordinatorResponse)

	initProducerIdResponse := new(InitProducerIDResponse)
	leader.Returns(initProducerIdResponse)

	addPartitionToTxn := new(AddPartitionsToTxnResponse)
	addPartitionToTxn.Errors = map[string][]*PartitionError{
		"my_topic": {
			{
				Partition: 0,
			},
		},
	}
	leader.Returns(addPartitionToTxn)

	prodSuccess := new(ProduceResponse)
	prodSuccess.Version = 3
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	for range 10 {
		leader.Returns(prodSuccess)
	}

	endTxnResponse := &EndTxnResponse{}
	leader.Returns(endTxnResponse)

	producer, err := NewSyncProducerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	if !producer.IsTransactional() {
		t.Error("producer is not transactional")
	}

	err = producer.BeginTxn()
	if err != nil {
		t.Fatal(err)
	}
	if producer.TxnStatus()&ProducerTxnFlagInTransaction == 0 {
		t.Error("transaction must started")
	}

	for range 10 {
		msg := &ProducerMessage{
			Topic:    "my_topic",
			Value:    StringEncoder(TestMessage),
			Metadata: "test",
		}

		partition, offset, err := producer.SendMessage(msg)

		if partition != 0 || msg.Partition != partition {
			t.Error("Unexpected partition")
		}
		if offset != 0 || msg.Offset != offset {
			t.Error("Unexpected offset")
		}
		if str, ok := msg.Metadata.(string); !ok || str != "test" {
			t.Error("Unexpected metadata")
		}
		if err != nil {
			t.Error(err)
		}
	}
	err = producer.CommitTxn()
	if err != nil {
		t.Fatal(err)
	}

	safeClose(t, producer)
}

func TestSyncProducerBatch(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 3
	config.Producer.Return.Successes = true
	producer, err := NewSyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	err = producer.SendMessages([]*ProducerMessage{
		{
			Topic:    "my_topic",
			Value:    StringEncoder(TestMessage),
			Metadata: "test",
		},
		{
			Topic:    "my_topic",
			Value:    StringEncoder(TestMessage),
			Metadata: "test",
		},
		{
			Topic:    "my_topic",
			Value:    StringEncoder(TestMessage),
			Metadata: "test",
		},
	})

	if err != nil {
		t.Error(err)
	}

	safeClose(t, producer)
	leader.Close()
	seedBroker.Close()
}

func TestConcurrentSyncProducer(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)

	config := NewTestConfig()
	config.Producer.Flush.Messages = 100
	config.Producer.Return.Successes = true
	producer, err := NewSyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}

	for range 100 {
		wg.Go(func() {
			msg := &ProducerMessage{Topic: "my_topic", Value: StringEncoder(TestMessage)}
			partition, _, err := producer.SendMessage(msg)
			if partition != 0 {
				t.Error("Unexpected partition")
			}
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	safeClose(t, producer)
	leader.Close()
	seedBroker.Close()
}

func TestSyncProducerToNonExistingTopic(t *testing.T) {
	broker := NewMockBroker(t, 1)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(broker.Addr(), broker.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)
	broker.Returns(metadataResponse)

	config := NewTestConfig()
	config.Metadata.Retry.Max = 0
	config.Producer.Retry.Max = 0
	config.Producer.Return.Successes = true

	producer, err := NewSyncProducer([]string{broker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	metadataResponse = new(MetadataResponse)
	metadataResponse.AddBroker(broker.Addr(), broker.BrokerID())
	metadataResponse.AddTopic("unknown", ErrUnknownTopicOrPartition)
	broker.Returns(metadataResponse)

	_, _, err = producer.SendMessage(&ProducerMessage{Topic: "unknown"})
	if !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Error("Uxpected ErrUnknownTopicOrPartition, found:", err)
	}

	safeClose(t, producer)
	broker.Close()
}

func TestSyncProducerRecoveryWithRetriesDisabled(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader1 := NewMockBroker(t, 2)
	leader2 := NewMockBroker(t, 3)

	metadataLeader1 := new(MetadataResponse)
	metadataLeader1.AddBroker(leader1.Addr(), leader1.BrokerID())
	metadataLeader1.AddTopicPartition("my_topic", 0, leader1.BrokerID(), nil, nil, nil, ErrNoError)
	seedBroker.Returns(metadataLeader1)

	config := NewTestConfig()
	config.Producer.Retry.Max = 0 // disable!
	config.Producer.Retry.Backoff = 0
	config.Producer.Return.Successes = true
	producer, err := NewSyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()

	prodNotLeader := new(ProduceResponse)
	prodNotLeader.AddTopicPartition("my_topic", 0, ErrNotLeaderForPartition)
	leader1.Returns(prodNotLeader)
	_, _, err = producer.SendMessage(&ProducerMessage{Topic: "my_topic", Value: StringEncoder(TestMessage)})
	if !errors.Is(err, ErrNotLeaderForPartition) {
		t.Fatal(err)
	}

	metadataLeader2 := new(MetadataResponse)
	metadataLeader2.AddBroker(leader2.Addr(), leader2.BrokerID())
	metadataLeader2.AddTopicPartition("my_topic", 0, leader2.BrokerID(), nil, nil, nil, ErrNoError)
	leader1.Returns(metadataLeader2)
	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader2.Returns(prodSuccess)
	_, _, err = producer.SendMessage(&ProducerMessage{Topic: "my_topic", Value: StringEncoder(TestMessage)})
	if err != nil {
		t.Fatal(err)
	}

	leader1.Close()
	leader2.Close()
	safeClose(t, producer)
}

func TestSyncProducerContext(t *testing.T) {
	// onProduce runs in the leader's handler goroutine for each produce request
	newProducer := func(t *testing.T, config *Config, onProduce func()) SyncProducerContext {
		seedBroker := NewMockBroker(t, 1)
		leader := NewMockBroker(t, 2)

		metadataResponse := new(MetadataResponse)
		metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
		metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
		seedBroker.Returns(metadataResponse)

		prodSuccess := new(ProduceResponse)
		prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
		leader.setHandler(func(req *request) encoderWithHeader {
			if req.body.key() == 3 {
				return metadataResponse
			}
			onProduce()
			return prodSuccess
		})

		config.Producer.Return.Successes = true
		producer, err := NewSyncProducer([]string{seedBroker.Addr()}, config)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			safeClose(t, producer)
			leader.Close()
			seedBroker.Close()
		})
		return producer.(SyncProducerContext)
	}

	// cancels ctx once a produce request arrives and holds the response until the test ends
	cancelOnProduce := func(t *testing.T, config *Config) (SyncProducerContext, context.Context) {
		ctx, cancel := context.WithCancel(t.Context())
		release := make(chan struct{})
		producer := newProducer(t, config, func() {
			cancel()
			<-release
		})
		// registered after the producer's cleanup so it runs first and lets Close finish
		t.Cleanup(func() { close(release) })
		return producer, ctx
	}

	newMessage := func() *ProducerMessage {
		return &ProducerMessage{Topic: "my_topic", Value: StringEncoder(TestMessage)}
	}

	// a producer whose input is a bare channel, so the test controls submission and results
	newStubProducer := func() (*syncProducer, chan *ProducerMessage) {
		input := make(chan *ProducerMessage)
		return &syncProducer{producer: &asyncProducer{input: input}}, input
	}

	t.Run("single send canceled after submission", func(t *testing.T) {
		producer, ctx := cancelOnProduce(t, NewTestConfig())

		_, _, err := producer.SendMessageContext(ctx, newMessage())
		if !errors.Is(err, context.Canceled) {
			t.Error("Expected context.Canceled, found:", err)
		}
		if errors.Is(err, ErrMessageNotSubmitted) {
			t.Error("Expected a submitted message not to be reported as unsubmitted, found:", err)
		}
	})

	t.Run("batch canceled after submission", func(t *testing.T) {
		config := NewTestConfig()
		config.Producer.Flush.Messages = 3
		producer, ctx := cancelOnProduce(t, config)

		err := producer.SendMessagesContext(ctx, []*ProducerMessage{newMessage(), newMessage(), newMessage()})
		if !errors.Is(err, context.Canceled) {
			t.Error("Expected context.Canceled, found:", err)
		}
		pErrs, ok := errors.AsType[ProducerErrors](err)
		if !ok || len(pErrs) != 3 {
			t.Fatal("Expected all 3 messages listed as unfinished, found:", err)
		}
		for _, pErr := range pErrs {
			if !errors.Is(pErr.Err, context.Canceled) || errors.Is(pErr.Err, ErrMessageNotSubmitted) {
				t.Error("Expected cancellation of a submitted message, found:", pErr.Err)
			}
		}
	})

	t.Run("context already done", func(t *testing.T) {
		producer := newProducer(t, NewTestConfig(), func() {
			t.Error("Expected no produce requests")
		})

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		msg := newMessage()
		_, _, err := producer.SendMessageContext(ctx, msg)
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrMessageNotSubmitted) {
			t.Error("Expected context.Canceled and ErrMessageNotSubmitted, found:", err)
		}
		err = producer.SendMessagesContext(ctx, []*ProducerMessage{msg})
		if !errors.Is(err, context.Canceled) {
			t.Error("Expected context.Canceled, found:", err)
		}
		pErrs, ok := errors.AsType[ProducerErrors](err)
		if !ok || len(pErrs) != 1 || pErrs[0].Msg != msg {
			t.Fatal("Expected the unsent message listed, found:", err)
		}
		if !errors.Is(pErrs[0].Err, ErrMessageNotSubmitted) {
			t.Error("Expected ErrMessageNotSubmitted, found:", pErrs[0].Err)
		}
	})

	t.Run("cancellation while input is blocked", func(t *testing.T) {
		producer, input := newStubProducer()
		msgs := []*ProducerMessage{{Topic: "held"}, {Topic: "unsent"}}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- producer.SendMessagesContext(ctx, msgs) }()
		held := <-input
		expectation := held.expectation
		cancel()
		err := <-result
		pErrs, _ := errors.AsType[ProducerErrors](err)
		if !errors.Is(err, context.Canceled) || len(pErrs) != len(msgs) {
			t.Fatal("Expected both messages listed on cancellation, found:", err)
		}
		for i, pErr := range pErrs {
			if pErr.Msg != msgs[i] || !errors.Is(pErr.Err, context.Canceled) {
				t.Error("Expected cancellation for message", i, "found:", pErr)
			}
		}
		if errors.Is(pErrs[0].Err, ErrMessageNotSubmitted) {
			t.Error("Expected the held message not to be reported as unsubmitted, found:", pErrs[0].Err)
		}
		if !errors.Is(pErrs[1].Err, ErrMessageNotSubmitted) {
			t.Error("Expected ErrMessageNotSubmitted for the unsent message, found:", pErrs[1].Err)
		}
		expectation <- nil

		// a message that never reached the producer remains safe to send
		go func() {
			_, _, err := producer.SendMessageContext(t.Context(), msgs[1])
			result <- err
		}()
		msg := <-input
		msg.expectation <- nil
		if err := <-result; err != nil {
			t.Error("Expected the unsent message to succeed on retry, found:", err)
		}
	})

	t.Run("completed results while input is blocked", func(t *testing.T) {
		producer, input := newStubProducer()
		msgs := []*ProducerMessage{{Topic: "rejected"}, {Topic: "succeeded"}, {Topic: "unsent"}}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- producer.SendMessagesContext(ctx, msgs) }()

		rejection := ConfigurationError("rejected message")
		rejected := <-input
		rejected.expectation <- &ProducerError{Msg: rejected, Err: rejection}
		succeeded := <-input
		succeeded.expectation <- nil
		cancel()

		err := <-result
		if !errors.Is(err, context.Canceled) {
			t.Error("Expected context.Canceled, found:", err)
		}
		failures, ok := errors.AsType[ProducerErrors](err)
		if !ok || len(failures) != 2 {
			t.Fatal("Expected only the rejected and unsent messages, found:", err)
		}
		if failures[0].Msg != rejected || !errors.Is(failures[0].Err, rejection) {
			t.Error("Expected the original rejection, found:", failures[0])
		}
		if failures[1].Msg != msgs[2] || !errors.Is(failures[1].Err, context.Canceled) || !errors.Is(failures[1].Err, ErrMessageNotSubmitted) {
			t.Error("Expected cancellation for the unsent message, found:", failures[1])
		}
	})

	// complete both outcomes before cancellation, regardless of result consumption order
	for _, tc := range []struct {
		name           string
		completedFirst bool
	}{
		{"completed results before the unfinished message", true},
		{"completed results after the unfinished message", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			producer, input := newStubProducer()
			rejected := &ProducerMessage{Topic: "rejected"}
			succeeded := &ProducerMessage{Topic: "succeeded"}
			held := &ProducerMessage{Topic: "held"}
			msgs := []*ProducerMessage{held, rejected, succeeded}
			if tc.completedFirst {
				msgs = []*ProducerMessage{rejected, succeeded, held}
			}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- producer.SendMessagesContext(ctx, msgs) }()

			expectations := make(map[*ProducerMessage]chan *ProducerError)
			for range msgs {
				msg := <-input
				expectations[msg] = msg.expectation
			}
			rejection := ConfigurationError("rejected message")
			expectations[rejected] <- &ProducerError{Msg: rejected, Err: rejection}
			expectations[succeeded] <- nil
			cancel()
			err := <-result

			// a late result must not block after the caller has returned
			select {
			case expectations[held] <- nil:
			default:
				t.Fatal("Late result blocked")
			}
			if !errors.Is(err, context.Canceled) {
				t.Error("Expected context.Canceled, found:", err)
			}
			pErrs, _ := errors.AsType[ProducerErrors](err)
			if len(pErrs) != 2 {
				t.Fatal("Expected only the rejected and unfinished messages, found:", err)
			}
			failures := make(map[*ProducerMessage]error)
			for _, pErr := range pErrs {
				failures[pErr.Msg] = pErr.Err
			}
			if !errors.Is(failures[held], context.Canceled) || errors.Is(failures[held], ErrMessageNotSubmitted) {
				t.Error("Expected cancellation of the submitted held message, found:", failures[held])
			}
			if !errors.Is(failures[rejected], rejection) {
				t.Error("Expected the original rejection, found:", failures[rejected])
			}
		})
	}
}

// This example shows the basic usage pattern of the SyncProducer.
func ExampleSyncProducer() {
	producer, err := NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	msg := &ProducerMessage{Topic: "my_topic", Value: StringEncoder("testing 123")}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("FAILED to send message: %s\n", err)
	} else {
		log.Printf("> message sent to partition %d at offset %d\n", partition, offset)
	}
}

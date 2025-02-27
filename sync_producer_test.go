//go:build !functional

package sarama

import (
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
	for i := 0; i < 10; i++ {
		leader.Returns(prodSuccess)
	}

	config := NewTestConfig()
	config.Producer.Return.Successes = true
	producer, err := NewSyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
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
	for i := 0; i < 10; i++ {
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

	for i := 0; i < 10; i++ {
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

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			msg := &ProducerMessage{Topic: "my_topic", Value: StringEncoder(TestMessage)}
			partition, _, err := producer.SendMessage(msg)
			if partition != 0 {
				t.Error("Unexpected partition")
			}
			if err != nil {
				t.Error(err)
			}
			wg.Done()
		}()
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

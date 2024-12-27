//go:build !functional

package sarama

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"
)

func TestSimpleClient(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	seedBroker.Returns(metadataResponse)

	client, err := NewClient([]string{seedBroker.Addr()}, NewTestConfig())
	if err != nil {
		t.Fatal(err)
	}

	seedBroker.Close()
	safeClose(t, client)
}

func TestCachedPartitions(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)

	replicas := []int32{3, 1, 5}
	isr := []int32{5, 1}

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker("localhost:12345", 2)
	metadataResponse.AddTopicPartition("my_topic", 0, 2, replicas, isr, []int32{}, ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 1, 2, replicas, isr, []int32{}, ErrLeaderNotAvailable)
	seedBroker.Returns(metadataResponse)

	config := NewTestConfig()
	config.Metadata.Retry.Max = 0
	c, err := NewClient([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, c)
	client := c.(*client)

	// Verify they aren't cached the same
	allP := client.cachedPartitionsResults["my_topic"][allPartitions]
	writeP := client.cachedPartitionsResults["my_topic"][writablePartitions]
	if len(allP) == len(writeP) {
		t.Fatal("Invalid lengths!")
	}

	tmp := client.cachedPartitionsResults["my_topic"]
	// Verify we actually use the cache at all!
	tmp[allPartitions] = []int32{1, 2, 3, 4}
	client.cachedPartitionsResults["my_topic"] = tmp
	if len(client.cachedPartitions("my_topic", allPartitions)) != 4 {
		t.Fatal("Not using the cache!")
	}

	seedBroker.Close()
}

func TestClientDoesntCachePartitionsForTopicsWithErrors(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)

	replicas := []int32{seedBroker.BrokerID()}

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 1, replicas[0], replicas, replicas, []int32{}, ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 2, replicas[0], replicas, replicas, []int32{}, ErrNoError)
	seedBroker.Returns(metadataResponse)

	config := NewTestConfig()
	config.Metadata.Retry.Max = 0
	client, err := NewClient([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	metadataResponse = new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadataResponse.AddTopic("unknown", ErrUnknownTopicOrPartition)
	seedBroker.Returns(metadataResponse)

	partitions, err := client.Partitions("unknown")

	if !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Error("Expected ErrUnknownTopicOrPartition, found", err)
	}
	if partitions != nil {
		t.Errorf("Should return nil as partition list, found %v", partitions)
	}

	// Should still use the cache of a known topic
	_, err = client.Partitions("my_topic")
	if err != nil {
		t.Errorf("Expected no error, found %v", err)
	}

	metadataResponse = new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadataResponse.AddTopic("unknown", ErrUnknownTopicOrPartition)
	seedBroker.Returns(metadataResponse)

	// Should not use cache for unknown topic
	partitions, err = client.Partitions("unknown")
	if !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Error("Expected ErrUnknownTopicOrPartition, found", err)
	}
	if partitions != nil {
		t.Errorf("Should return nil as partition list, found %v", partitions)
	}

	seedBroker.Close()
	safeClose(t, client)
}

func TestClientSeedBrokers(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker("localhost:12345", 2)
	seedBroker.Returns(metadataResponse)

	client, err := NewClient([]string{seedBroker.Addr()}, NewTestConfig())
	if err != nil {
		t.Fatal(err)
	}

	seedBroker.Close()
	safeClose(t, client)
}

func TestClientMetadata(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 5)

	replicas := []int32{3, 1, 5}
	isr := []int32{5, 1}

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), replicas, isr, []int32{}, ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader.BrokerID(), replicas, isr, []int32{}, ErrLeaderNotAvailable)
	seedBroker.Returns(metadataResponse)

	config := NewTestConfig()
	config.Metadata.Retry.Max = 0
	client, err := NewClient([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	topics, err := client.Topics()
	if err != nil {
		t.Error(err)
	} else if len(topics) != 1 || topics[0] != "my_topic" {
		t.Error("Client returned incorrect topics:", topics)
	}

	parts, err := client.Partitions("my_topic")
	if err != nil {
		t.Error(err)
	} else if len(parts) != 2 || parts[0] != 0 || parts[1] != 1 {
		t.Error("Client returned incorrect partitions for my_topic:", parts)
	}

	parts, err = client.WritablePartitions("my_topic")
	if err != nil {
		t.Error(err)
	} else if len(parts) != 1 || parts[0] != 0 {
		t.Error("Client returned incorrect writable partitions for my_topic:", parts)
	}

	tst, err := client.Leader("my_topic", 0)
	if err != nil {
		t.Error(err)
	} else if tst.ID() != 5 {
		t.Error("Leader for my_topic had incorrect ID.")
	}

	replicas, err = client.Replicas("my_topic", 0)
	if err != nil {
		t.Error(err)
	} else if replicas[0] != 3 {
		t.Error("Incorrect (or sorted) replica")
	} else if replicas[1] != 1 {
		t.Error("Incorrect (or sorted) replica")
	} else if replicas[2] != 5 {
		t.Error("Incorrect (or sorted) replica")
	}

	isr, err = client.InSyncReplicas("my_topic", 0)
	if err != nil {
		t.Error(err)
	} else if len(isr) != 2 {
		t.Error("Client returned incorrect ISRs for partition:", isr)
	} else if isr[0] != 5 {
		t.Error("Incorrect (or sorted) ISR:", isr)
	} else if isr[1] != 1 {
		t.Error("Incorrect (or sorted) ISR:", isr)
	}

	leader.Close()
	seedBroker.Close()
	safeClose(t, client)
}

func TestClientMetadataWithOfflineReplicas(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 5)

	replicas := []int32{1, 2, 3}
	isr := []int32{1, 2}
	offlineReplicas := []int32{3}

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), replicas, isr, offlineReplicas, ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader.BrokerID(), replicas, isr, []int32{}, ErrNoError)
	metadataResponse.Version = 5

	seedBroker.Returns(metadataResponse)

	config := NewTestConfig()
	config.Version = V1_0_0_0
	config.Metadata.Retry.Max = 0
	client, err := NewClient([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	topics, err := client.Topics()
	if err != nil {
		t.Error(err)
	} else if len(topics) != 1 || topics[0] != "my_topic" {
		t.Error("Client returned incorrect topics:", topics)
	}

	parts, err := client.Partitions("my_topic")
	if err != nil {
		t.Error(err)
	} else if len(parts) != 2 || parts[0] != 0 || parts[1] != 1 {
		t.Error("Client returned incorrect partitions for my_topic:", parts)
	}

	parts, err = client.WritablePartitions("my_topic")
	if err != nil {
		t.Error(err)
	} else if len(parts) != 2 {
		t.Error("Client returned incorrect writable partitions for my_topic:", parts)
	}

	tst, err := client.Leader("my_topic", 0)
	if err != nil {
		t.Error(err)
	} else if tst.ID() != 5 {
		t.Error("Leader for my_topic had incorrect ID.")
	}

	replicas, err = client.Replicas("my_topic", 0)
	if err != nil {
		t.Error(err)
	} else if replicas[0] != 1 {
		t.Error("Incorrect (or sorted) replica")
	} else if replicas[1] != 2 {
		t.Error("Incorrect (or sorted) replica")
	} else if replicas[2] != 3 {
		t.Error("Incorrect (or sorted) replica")
	}

	isr, err = client.InSyncReplicas("my_topic", 0)
	if err != nil {
		t.Error(err)
	} else if len(isr) != 2 {
		t.Error("Client returned incorrect ISRs for partition:", isr)
	} else if isr[0] != 1 {
		t.Error("Incorrect (or sorted) ISR:", isr)
	} else if isr[1] != 2 {
		t.Error("Incorrect (or sorted) ISR:", isr)
	}

	offlineReplicas, err = client.OfflineReplicas("my_topic", 0)
	if err != nil {
		t.Error(err)
	} else if len(offlineReplicas) != 1 {
		t.Error("Client returned incorrect offline replicas for partition:", offlineReplicas)
	} else if offlineReplicas[0] != 3 {
		t.Error("Incorrect offline replica:", offlineReplicas)
	}

	leader.Close()
	seedBroker.Close()
	safeClose(t, client)
}

func TestClientGetOffset(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)
	leaderAddr := leader.Addr()

	metadata := new(MetadataResponse)
	metadata.AddTopicPartition("foo", 0, leader.BrokerID(), nil, nil, nil, ErrNoError)
	metadata.AddBroker(leaderAddr, leader.BrokerID())
	seedBroker.Returns(metadata)

	client, err := NewClient([]string{seedBroker.Addr()}, NewTestConfig())
	if err != nil {
		t.Fatal(err)
	}

	offsetResponse := new(OffsetResponse)
	offsetResponse.AddTopicPartition("foo", 0, 123)
	leader.Returns(offsetResponse)

	offset, err := client.GetOffset("foo", 0, OffsetNewest)
	if err != nil {
		t.Error(err)
	}
	if offset != 123 {
		t.Error("Unexpected offset, got ", offset)
	}

	leader.Close()

	leader = NewMockBrokerAddr(t, 2, leaderAddr)
	offsetResponse = new(OffsetResponse)
	offsetResponse.AddTopicPartition("foo", 0, 456)
	leader.Returns(metadata)
	leader.Returns(offsetResponse)

	offset, err = client.GetOffset("foo", 0, OffsetNewest)
	if err != nil {
		t.Error(err)
	}
	if offset != 456 {
		t.Error("Unexpected offset, got ", offset)
	}

	seedBroker.Close()
	leader.Close()
	safeClose(t, client)
}

func TestClientReceivingUnknownTopicWithBackoffFunc(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)

	metadataResponse1 := new(MetadataResponse)
	metadataResponse1.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	seedBroker.Returns(metadataResponse1)

	retryCount := int32(0)

	config := NewTestConfig()
	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.BackoffFunc = func(retries, maxRetries int) time.Duration {
		atomic.AddInt32(&retryCount, 1)
		return 0
	}
	client, err := NewClient([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	metadataUnknownTopic := new(MetadataResponse)
	metadataUnknownTopic.AddTopic("new_topic", ErrUnknownTopicOrPartition)
	metadataUnknownTopic.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	seedBroker.Returns(metadataUnknownTopic)
	seedBroker.Returns(metadataUnknownTopic)

	if err := client.RefreshMetadata("new_topic"); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Error("ErrUnknownTopicOrPartition expected, got", err)
	}

	safeClose(t, client)
	seedBroker.Close()

	actualRetryCount := atomic.LoadInt32(&retryCount)
	if actualRetryCount != 1 {
		t.Fatalf("Expected BackoffFunc to be called exactly once, but saw %d", actualRetryCount)
	}
}

func TestClientReceivingUnknownTopic(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)

	metadataResponse1 := new(MetadataResponse)
	metadataResponse1.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	seedBroker.Returns(metadataResponse1)

	config := NewTestConfig()
	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 0
	client, err := NewClient([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	metadataUnknownTopic := new(MetadataResponse)
	metadataUnknownTopic.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadataUnknownTopic.AddTopic("new_topic", ErrUnknownTopicOrPartition)
	seedBroker.Returns(metadataUnknownTopic)
	seedBroker.Returns(metadataUnknownTopic)

	if err := client.RefreshMetadata("new_topic"); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Error("ErrUnknownTopicOrPartition expected, got", err)
	}

	// If we are asking for the leader of a partition of the non-existing topic.
	// we will request metadata again.
	seedBroker.Returns(metadataUnknownTopic)
	seedBroker.Returns(metadataUnknownTopic)

	if _, err = client.Leader("new_topic", 1); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Error("Expected ErrUnknownTopicOrPartition, got", err)
	}

	safeClose(t, client)
	seedBroker.Close()
}

func TestClientReceivingPartialMetadata(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 5)

	metadataResponse1 := new(MetadataResponse)
	metadataResponse1.AddBroker(leader.Addr(), leader.BrokerID())
	seedBroker.Returns(metadataResponse1)

	config := NewTestConfig()
	config.Metadata.Retry.Max = 0
	client, err := NewClient([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	replicas := []int32{leader.BrokerID(), seedBroker.BrokerID()}

	metadataPartial := new(MetadataResponse)
	metadataPartial.AddBroker(leader.Addr(), 5)
	metadataPartial.AddTopic("new_topic", ErrLeaderNotAvailable)
	metadataPartial.AddTopicPartition("new_topic", 0, leader.BrokerID(), replicas, replicas, []int32{}, ErrNoError)
	metadataPartial.AddTopicPartition("new_topic", 1, -1, replicas, []int32{}, []int32{}, ErrLeaderNotAvailable)
	leader.Returns(metadataPartial)

	if err := client.RefreshMetadata("new_topic"); err != nil {
		t.Error("ErrLeaderNotAvailable should not make RefreshMetadata respond with an error")
	}

	// Even though the metadata was incomplete, we should be able to get the leader of a partition
	// for which we did get a useful response, without doing additional requests.

	partition0Leader, err := client.Leader("new_topic", 0)
	if err != nil {
		t.Error(err)
	} else if partition0Leader.Addr() != leader.Addr() {
		t.Error("Unexpected leader returned", partition0Leader.Addr())
	}

	// If we are asking for the leader of a partition that didn't have a leader before,
	// we will do another metadata request.

	leader.Returns(metadataPartial)

	// Still no leader for the partition, so asking for it should return an error.
	_, err = client.Leader("new_topic", 1)
	if !errors.Is(err, ErrLeaderNotAvailable) {
		t.Error("Expected ErrLeaderNotAvailable, got", err)
	}

	safeClose(t, client)
	seedBroker.Close()
	leader.Close()
}

func TestClientRefreshBehaviourWhenEmptyMetadataResponse(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	broker := NewMockBroker(t, 2)

	metadataResponse1 := new(MetadataResponse)
	metadataResponse1.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	seedBroker.Returns(metadataResponse1)

	c, err := NewClient([]string{seedBroker.Addr()}, NewTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	client := c.(*client)
	if len(client.seedBrokers) != 1 {
		t.Error("incorrect number of live seeds")
	}
	if len(client.deadSeeds) != 0 {
		t.Error("incorrect number of dead seeds")
	}
	if len(client.brokers) != 1 {
		t.Error("incorrect number of brokers")
	}

	// Empty metadata response
	seedBroker.Returns(new(MetadataResponse))
	metadataResponse2 := new(MetadataResponse)
	metadataResponse2.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadataResponse2.AddBroker(broker.Addr(), broker.BrokerID())
	seedBroker.Returns(metadataResponse2)
	err = c.RefreshMetadata()
	if err != nil {
		t.Fatal(err)
	}
	if len(client.seedBrokers) != 1 {
		t.Error("incorrect number of live seeds")
	}
	if len(client.deadSeeds) != 0 {
		t.Error("incorrect number of dead seeds")
	}
	if len(client.brokers) != 2 {
		t.Error("incorrect number of brokers")
	}
	broker.Close()
	seedBroker.Close()
	safeClose(t, client)
}

func TestClientRefreshBehaviour(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 5)

	metadataResponse1 := new(MetadataResponse)
	metadataResponse1.AddBroker(leader.Addr(), leader.BrokerID())
	seedBroker.Returns(metadataResponse1)

	metadataResponse2 := new(MetadataResponse)
	metadataResponse2.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse2.AddTopicPartition("my_topic", 0xb, leader.BrokerID(), nil, nil, nil, ErrNoError)
	leader.Returns(metadataResponse2)

	client, err := NewClient([]string{seedBroker.Addr()}, NewTestConfig())
	if err != nil {
		t.Fatal(err)
	}

	parts, err := client.Partitions("my_topic")
	if err != nil {
		t.Error(err)
	} else if len(parts) != 1 || parts[0] != 0xb {
		t.Error("Client returned incorrect partitions for my_topic:", parts)
	}

	tst, err := client.Leader("my_topic", 0xb)
	if err != nil {
		t.Error(err)
	} else if tst.ID() != 5 {
		t.Error("Leader for my_topic had incorrect ID.")
	}

	leader.Close()
	seedBroker.Close()
	safeClose(t, client)
}

func TestClientRefreshBrokers(t *testing.T) {
	initialSeed := NewMockBroker(t, 0)
	defer initialSeed.Close()
	leader := NewMockBroker(t, 5)
	defer leader.Close()

	metadataResponse1 := new(MetadataResponse)
	metadataResponse1.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse1.AddBroker(initialSeed.Addr(), initialSeed.BrokerID())
	initialSeed.Returns(metadataResponse1)

	c, err := NewClient([]string{initialSeed.Addr()}, NewTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	client := c.(*client)

	if len(client.Brokers()) != 2 {
		t.Error("Meta broker is not 2")
	}

	newSeedBrokers := []string{"localhost:12345"}
	_ = client.RefreshBrokers(newSeedBrokers)

	if client.seedBrokers[0].addr != newSeedBrokers[0] {
		t.Error("Seed broker not updated")
	}
	if len(client.Brokers()) != 0 {
		t.Error("Old brokers not closed")
	}
}

func TestClientRefreshMetadataBrokerOffline(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()
	leader := NewMockBroker(t, 5)
	defer leader.Close()

	metadataResponse1 := new(MetadataResponse)
	metadataResponse1.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse1.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	seedBroker.Returns(metadataResponse1)

	client, err := NewClient([]string{seedBroker.Addr()}, NewTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if len(client.Brokers()) != 2 {
		t.Error("Meta broker is not 2")
	}

	metadataResponse2 := NewMockMetadataResponse(t).SetBroker(leader.Addr(), leader.BrokerID())
	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": metadataResponse2,
	})
	leader.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": metadataResponse2,
	})

	if err := client.RefreshMetadata(); err != nil {
		t.Error(err)
	}
	if len(client.Brokers()) != 1 {
		t.Error("Meta broker is not 1")
	}
}

func TestClientGetBroker(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()
	leader := NewMockBroker(t, 5)
	defer leader.Close()

	metadataResponse1 := new(MetadataResponse)
	metadataResponse1.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse1.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	seedBroker.Returns(metadataResponse1)

	client, err := NewClient([]string{seedBroker.Addr()}, NewTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	broker, err := client.Broker(leader.BrokerID())
	if err != nil {
		t.Fatal(err)
	}

	if broker.Addr() != leader.Addr() {
		t.Errorf("Expected broker to have address %s, found %s", leader.Addr(), broker.Addr())
	}

	metadataResponse2 := NewMockMetadataResponse(t).SetBroker(seedBroker.Addr(), seedBroker.BrokerID())
	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": metadataResponse2,
	})
	leader.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": metadataResponse2,
	})

	if err := client.RefreshMetadata(); err != nil {
		t.Error(err)
	}
	_, err = client.Broker(leader.BrokerID())
	if !errors.Is(err, ErrBrokerNotFound) {
		t.Errorf("Expected Broker(brokerID) to return %v found %v", ErrBrokerNotFound, err)
	}
}

func TestClientResurrectDeadSeeds(t *testing.T) {
	initialSeed := NewMockBroker(t, 0)
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(initialSeed.Addr(), initialSeed.BrokerID())
	initialSeed.Returns(metadataResponse)

	conf := NewTestConfig()
	conf.Metadata.Retry.Backoff = 0
	conf.Metadata.RefreshFrequency = 0
	c, err := NewClient([]string{initialSeed.Addr()}, conf)
	if err != nil {
		t.Fatal(err)
	}

	client := c.(*client)

	seed1 := NewMockBroker(t, 1)
	seed2 := NewMockBroker(t, 2)
	seed3 := NewMockBroker(t, 3)
	addr1 := seed1.Addr()
	addr2 := seed2.Addr()
	addr3 := seed3.Addr()

	// Overwrite the seed brokers with a fixed ordering to make this test deterministic.
	safeClose(t, client.seedBrokers[0])
	client.seedBrokers = []*Broker{NewBroker(addr1), NewBroker(addr2), NewBroker(addr3)}
	client.deadSeeds = []*Broker{}
	client.brokers = map[int32]*Broker{}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		if err := client.RefreshMetadata(); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()
	seed1.Close()
	seed2.Close()

	seed1 = NewMockBrokerAddr(t, 1, addr1)
	seed2 = NewMockBrokerAddr(t, 2, addr2)

	seed3.Close()

	seed1.Close()
	metadataResponse2 := new(MetadataResponse)
	metadataResponse2.AddBroker(seed2.Addr(), seed2.BrokerID())
	seed2.Returns(metadataResponse2)

	wg.Wait()

	if len(client.seedBrokers) != 2 {
		t.Error("incorrect number of live seeds")
	}
	if len(client.deadSeeds) != 1 {
		t.Error("incorrect number of dead seeds")
	}

	seed2.Close()
	safeClose(t, c)
}

//nolint:paralleltest
func TestClientController(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()
	controllerBroker := NewMockBroker(t, 2)
	defer controllerBroker.Close()

	seedBroker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(controllerBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(controllerBroker.Addr(), controllerBroker.BrokerID()),
	})

	cfg := NewTestConfig()

	// test kafka version greater than 0.10.0.0
	t.Run("V0_10_0_0", func(t *testing.T) {
		cfg.Version = V0_10_0_0
		client1, err := NewClient([]string{seedBroker.Addr()}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer safeClose(t, client1)
		broker, err := client1.Controller()
		if err != nil {
			t.Fatal(err)
		}
		if broker.Addr() != controllerBroker.Addr() {
			t.Errorf("Expected controller to have address %s, found %s", controllerBroker.Addr(), broker.Addr())
		}
	})

	// test kafka version earlier than 0.10.0.0
	t.Run("V0_9_0_1", func(t *testing.T) {
		cfg.Version = V0_9_0_1
		client2, err := NewClient([]string{seedBroker.Addr()}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer safeClose(t, client2)
		if _, err = client2.Controller(); !errors.Is(err, ErrUnsupportedVersion) {
			t.Errorf("Expected Controller() to return %s, found %s", ErrUnsupportedVersion, err)
		}
	})
}

func TestClientMetadataTimeout(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{
			"timeout=250ms",
			250 * time.Millisecond, // Will cut the first retry pass
		},
		{
			"timeout=500ms",
			500 * time.Millisecond, // Will cut the second retry pass
		},
		{
			"timeout=750ms",
			750 * time.Millisecond, // Will cut the third retry pass
		},
		{
			"timeout=900ms",
			900 * time.Millisecond, // Will stop after the three retries
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Use a responsive broker to create a working client
			initialSeed := NewMockBroker(t, 0)
			emptyMetadata := new(MetadataResponse)
			emptyMetadata.AddBroker(initialSeed.Addr(), initialSeed.BrokerID())
			initialSeed.Returns(emptyMetadata)

			conf := NewTestConfig()
			// Speed up the metadata request failure because of a read timeout
			conf.Net.ReadTimeout = 100 * time.Millisecond
			// Disable backoff and refresh
			conf.Metadata.Retry.Backoff = 0
			conf.Metadata.RefreshFrequency = 0
			// But configure a "global" timeout
			conf.Metadata.Timeout = tc.timeout
			c, err := NewClient([]string{initialSeed.Addr()}, conf)
			if err != nil {
				t.Fatal(err)
			}
			initialSeed.Close()

			client := c.(*client)

			// Start seed brokers that do not reply to anything and therefore a read
			// on the TCP connection will timeout to simulate unresponsive brokers
			seed1 := NewMockBroker(t, 1)
			defer seed1.Close()
			seed2 := NewMockBroker(t, 2)
			defer seed2.Close()

			// Overwrite the seed brokers with a fixed ordering to make this test deterministic
			safeClose(t, client.seedBrokers[0])
			client.seedBrokers = []*Broker{NewBroker(seed1.Addr()), NewBroker(seed2.Addr())}
			client.deadSeeds = []*Broker{}

			// Start refreshing metadata in the background
			errChan := make(chan error)
			go func() {
				errChan <- c.RefreshMetadata()
			}()

			// Check that the refresh fails fast enough (less than twice the configured timeout)
			// instead of at least: 100 ms * 2 brokers * 3 retries = 800 ms
			maxRefreshDuration := 2 * tc.timeout
			select {
			case err := <-errChan:
				if err == nil {
					t.Fatal("Expected failed RefreshMetadata, got nil")
				}
				if !errors.Is(err, ErrOutOfBrokers) {
					t.Error("Expected failed RefreshMetadata with ErrOutOfBrokers, got:", err)
				}
			case <-time.After(maxRefreshDuration):
				t.Fatalf("RefreshMetadata did not fail fast enough after waiting for %v", maxRefreshDuration)
			}

			safeClose(t, c)
		})
	}
}

func TestClientUpdateMetadataErrorAndRetry(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)

	metadataResponse1 := new(MetadataResponse)
	metadataResponse1.AddBroker(seedBroker.Addr(), 1)
	seedBroker.Returns(metadataResponse1)

	config := NewTestConfig()
	config.Metadata.Retry.Max = 3
	config.Metadata.Retry.Backoff = 200 * time.Millisecond
	config.Metadata.RefreshFrequency = 0
	config.Net.ReadTimeout = 10 * time.Millisecond
	config.Net.WriteTimeout = 10 * time.Millisecond
	client, err := NewClient([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer waitGroup.Done()
			var failedMetadataResponse MetadataResponse
			failedMetadataResponse.AddBroker(seedBroker.Addr(), 1)
			failedMetadataResponse.AddTopic("new_topic", ErrUnknownTopicOrPartition)
			seedBroker.Returns(&failedMetadataResponse)
			err := client.RefreshMetadata()
			if err == nil {
				t.Error("should return error")
				return
			}
		}()
	}
	waitGroup.Wait()
	safeClose(t, client)
	seedBroker.Close()
}

func TestClientCoordinatorWithConsumerOffsetsTopic(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	coordinator := NewMockBroker(t, 2)

	replicas := []int32{coordinator.BrokerID()}
	metadataResponse1 := new(MetadataResponse)
	metadataResponse1.AddBroker(coordinator.Addr(), coordinator.BrokerID())
	metadataResponse1.AddTopicPartition("__consumer_offsets", 0, replicas[0], replicas, replicas, []int32{}, ErrNoError)
	seedBroker.Returns(metadataResponse1)

	client, err := NewClient([]string{seedBroker.Addr()}, NewTestConfig())
	if err != nil {
		t.Fatal(err)
	}

	coordinatorResponse1 := new(ConsumerMetadataResponse)
	coordinatorResponse1.Err = ErrConsumerCoordinatorNotAvailable
	coordinator.Returns(coordinatorResponse1)

	coordinatorResponse2 := new(ConsumerMetadataResponse)
	coordinatorResponse2.CoordinatorID = coordinator.BrokerID()
	coordinatorResponse2.CoordinatorHost = "127.0.0.1"
	coordinatorResponse2.CoordinatorPort = coordinator.Port()

	coordinator.Returns(coordinatorResponse2)

	broker, err := client.Coordinator("my_group")
	if err != nil {
		t.Error(err)
	}

	if coordinator.Addr() != broker.Addr() {
		t.Errorf("Expected coordinator to have address %s, found %s", coordinator.Addr(), broker.Addr())
	}

	if coordinator.BrokerID() != broker.ID() {
		t.Errorf("Expected coordinator to have ID %d, found %d", coordinator.BrokerID(), broker.ID())
	}

	// Grab the cached value
	broker2, err := client.Coordinator("my_group")
	if err != nil {
		t.Error(err)
	}

	if broker2.Addr() != broker.Addr() {
		t.Errorf("Expected the coordinator to be the same, but found %s vs. %s", broker2.Addr(), broker.Addr())
	}

	coordinator.Close()
	seedBroker.Close()
	safeClose(t, client)
}

func TestClientCoordinatorChangeWithConsumerOffsetsTopic(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	staleCoordinator := NewMockBroker(t, 2)
	freshCoordinator := NewMockBroker(t, 3)

	replicas := []int32{staleCoordinator.BrokerID(), freshCoordinator.BrokerID()}
	metadataResponse1 := new(MetadataResponse)
	metadataResponse1.AddBroker(staleCoordinator.Addr(), staleCoordinator.BrokerID())
	metadataResponse1.AddBroker(freshCoordinator.Addr(), freshCoordinator.BrokerID())
	metadataResponse1.AddTopicPartition("__consumer_offsets", 0, replicas[0], replicas, replicas, []int32{}, ErrNoError)
	seedBroker.Returns(metadataResponse1)

	client, err := NewClient([]string{seedBroker.Addr()}, NewTestConfig())
	if err != nil {
		t.Fatal(err)
	}

	findCoordinatorResponse := NewMockFindCoordinatorResponse(t).SetCoordinator(CoordinatorGroup, "my_group", staleCoordinator)
	staleCoordinator.SetHandlerByMap(map[string]MockResponse{
		"FindCoordinatorRequest": findCoordinatorResponse,
	})
	freshCoordinator.SetHandlerByMap(map[string]MockResponse{
		"FindCoordinatorRequest": findCoordinatorResponse,
	})
	broker, err := client.Coordinator("my_group")
	if err != nil {
		t.Error(err)
	}

	if staleCoordinator.Addr() != broker.Addr() {
		t.Errorf("Expected coordinator to have address %s, found %s", staleCoordinator.Addr(), broker.Addr())
	}

	if staleCoordinator.BrokerID() != broker.ID() {
		t.Errorf("Expected coordinator to have ID %d, found %d", staleCoordinator.BrokerID(), broker.ID())
	}

	// Grab the cached value
	broker2, err := client.Coordinator("my_group")
	if err != nil {
		t.Error(err)
	}

	if broker2.Addr() != broker.Addr() {
		t.Errorf("Expected the coordinator to be the same, but found %s vs. %s", broker2.Addr(), broker.Addr())
	}

	findCoordinatorResponse2 := NewMockFindCoordinatorResponse(t).SetCoordinator(CoordinatorGroup, "my_group", freshCoordinator)
	staleCoordinator.SetHandlerByMap(map[string]MockResponse{
		"FindCoordinatorRequest": findCoordinatorResponse2,
	})
	freshCoordinator.SetHandlerByMap(map[string]MockResponse{
		"FindCoordinatorRequest": findCoordinatorResponse2,
	})

	// Refresh the locally cached value because it's stale
	if err := client.RefreshCoordinator("my_group"); err != nil {
		t.Error(err)
	}

	// Grab the fresh value
	broker3, err := client.Coordinator("my_group")
	if err != nil {
		t.Error(err)
	}

	if broker3.Addr() != freshCoordinator.Addr() {
		t.Errorf("Expected the freshCoordinator to be returned, but found %s.", broker3.Addr())
	}

	freshCoordinator.Close()
	staleCoordinator.Close()
	seedBroker.Close()
	safeClose(t, client)
}

func TestClientCoordinatorWithoutConsumerOffsetsTopic(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	coordinator := NewMockBroker(t, 2)

	metadataResponse1 := new(MetadataResponse)
	metadataResponse1.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	seedBroker.Returns(metadataResponse1)

	config := NewTestConfig()
	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 0
	client, err := NewClient([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	coordinatorResponse1 := new(ConsumerMetadataResponse)
	coordinatorResponse1.Err = ErrConsumerCoordinatorNotAvailable
	seedBroker.Returns(coordinatorResponse1)

	metadataResponse2 := new(MetadataResponse)
	metadataResponse2.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadataResponse2.AddTopic("__consumer_offsets", ErrUnknownTopicOrPartition)
	seedBroker.Returns(metadataResponse2)

	replicas := []int32{coordinator.BrokerID()}
	metadataResponse3 := new(MetadataResponse)
	metadataResponse3.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadataResponse3.AddTopicPartition("__consumer_offsets", 0, replicas[0], replicas, replicas, []int32{}, ErrNoError)
	seedBroker.Returns(metadataResponse3)

	coordinatorResponse2 := new(ConsumerMetadataResponse)
	coordinatorResponse2.CoordinatorID = coordinator.BrokerID()
	coordinatorResponse2.CoordinatorHost = "127.0.0.1"
	coordinatorResponse2.CoordinatorPort = coordinator.Port()

	seedBroker.Returns(coordinatorResponse2)

	broker, err := client.Coordinator("my_group")
	if err != nil {
		t.Error(err)
	}

	if coordinator.Addr() != broker.Addr() {
		t.Errorf("Expected coordinator to have address %s, found %s", coordinator.Addr(), broker.Addr())
	}

	if coordinator.BrokerID() != broker.ID() {
		t.Errorf("Expected coordinator to have ID %d, found %d", coordinator.BrokerID(), broker.ID())
	}

	coordinator.Close()
	seedBroker.Close()
	safeClose(t, client)
}

func TestClientAutorefreshShutdownRace(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	seedBroker.Returns(metadataResponse)

	conf := NewTestConfig()
	conf.Metadata.RefreshFrequency = 100 * time.Millisecond
	client, err := NewClient([]string{seedBroker.Addr()}, conf)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for the background refresh to kick in
	time.Sleep(110 * time.Millisecond)

	errCh := make(chan error, 1)
	go func() {
		// Close the client
		errCh <- client.Close()
		close(errCh)
	}()

	// Wait for the Close to kick in
	time.Sleep(10 * time.Millisecond)

	// Then return some metadata to the still-running background thread
	leader := NewMockBroker(t, 2)
	defer leader.Close()
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("foo", 0, leader.BrokerID(), []int32{2}, []int32{2}, []int32{}, ErrNoError)
	seedBroker.Returns(metadataResponse)

	err = <-errCh
	if err != nil {
		t.Fatalf("goroutine client.Close():%s", err)
	}

	// give the update time to happen so we get a panic if it's still running (which it shouldn't)
	time.Sleep(10 * time.Millisecond)
}

func TestClientConnectionRefused(t *testing.T) {
	t.Parallel()
	seedBroker := NewMockBroker(t, 1)
	seedBroker.Close()

	_, err := NewClient([]string{seedBroker.Addr()}, NewTestConfig())
	if !errors.Is(err, ErrOutOfBrokers) {
		t.Fatalf("unexpected error: %v", err)
	}

	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClientCoordinatorConnectionRefused(t *testing.T) {
	t.Parallel()
	seedBroker := NewMockBroker(t, 1)
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	seedBroker.Returns(metadataResponse)

	client, err := NewClient([]string{seedBroker.Addr()}, NewTestConfig())
	if err != nil {
		t.Fatal(err)
	}

	seedBroker.Close()

	_, err = client.Coordinator("my_group")

	if !errors.Is(err, ErrOutOfBrokers) {
		t.Fatalf("unexpected error: %v", err)
	}

	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("unexpected error: %v", err)
	}

	safeClose(t, client)
}

func TestInitProducerIDConnectionRefused(t *testing.T) {
	t.Parallel()
	seedBroker := NewMockBroker(t, 1)
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadataResponse.Version = 4
	seedBroker.Returns(metadataResponse)

	config := NewTestConfig()
	config.Producer.Idempotent = true
	config.Version = V0_11_0_0
	config.Producer.RequiredAcks = WaitForAll
	config.Net.MaxOpenRequests = 1

	client, err := NewClient([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	seedBroker.Close()

	_, err = client.InitProducerID()

	if !errors.Is(err, ErrOutOfBrokers) {
		t.Fatalf("unexpected error: %v", err)
	}

	if !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("unexpected error: %v", err)
	}

	safeClose(t, client)
}

func TestMetricsCleanup(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	defer seedBroker.Close()
	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	seedBroker.Returns(metadataResponse)

	config := NewTestConfig()
	metrics.GetOrRegisterMeter("a", config.MetricRegistry)

	client, err := NewClient([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	safeClose(t, client)

	// Wait async close
	time.Sleep(10 * time.Millisecond)

	all := config.MetricRegistry.GetAll()
	if len(all) != 1 || all["a"] == nil {
		t.Errorf("excepted 1 metric, found: %v", all)
	}
}

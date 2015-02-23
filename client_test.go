package sarama

import (
	"io"
	"testing"
)

func safeClose(t *testing.T, c io.Closer) {
	err := c.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestDefaultClientConfigValidates(t *testing.T) {
	config := NewClientConfig()
	if err := config.Validate(); err != nil {
		t.Error(err)
	}
}

func TestSimpleClient(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)

	seedBroker.Returns(new(MetadataResponse))

	client, err := NewClient("client_id", []string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	seedBroker.Close()
	safeClose(t, client)
}

func TestCachedPartitions(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 5)

	replicas := []int32{3, 1, 5}
	isr := []int32{5, 1}

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), replicas, isr, NoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader.BrokerID(), replicas, isr, LeaderNotAvailable)
	seedBroker.Returns(metadataResponse)

	config := NewClientConfig()
	config.MetadataRetries = 0
	client, err := NewClient("client_id", []string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

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
	if 4 != len(client.cachedPartitions("my_topic", allPartitions)) {
		t.Fatal("Not using the cache!")
	}

	leader.Close()
	seedBroker.Close()
	safeClose(t, client)
}

func TestClientSeedBrokers(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	discoveredBroker := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(discoveredBroker.Addr(), discoveredBroker.BrokerID())
	seedBroker.Returns(metadataResponse)

	client, err := NewClient("client_id", []string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	discoveredBroker.Close()
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
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), replicas, isr, NoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader.BrokerID(), replicas, isr, LeaderNotAvailable)
	seedBroker.Returns(metadataResponse)

	config := NewClientConfig()
	config.MetadataRetries = 0
	client, err := NewClient("client_id", []string{seedBroker.Addr()}, config)
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
	} else if replicas[0] != 1 {
		t.Error("Incorrect (or unsorted) replica")
	} else if replicas[1] != 3 {
		t.Error("Incorrect (or unsorted) replica")
	} else if replicas[2] != 5 {
		t.Error("Incorrect (or unsorted) replica")
	}

	isr, err = client.ReplicasInSync("my_topic", 0)
	if err != nil {
		t.Error(err)
	} else if isr[0] != 1 {
		t.Error("Incorrect (or unsorted) isr")
	} else if isr[1] != 5 {
		t.Error("Incorrect (or unsorted) isr")
	}

	leader.Close()
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
	metadataResponse2.AddTopicPartition("my_topic", 0xb, leader.BrokerID(), nil, nil, NoError)
	seedBroker.Returns(metadataResponse2)

	client, err := NewClient("clientID", []string{seedBroker.Addr()}, nil)
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

	client.disconnectBroker(tst)
	leader.Close()
	seedBroker.Close()
	safeClose(t, client)
}

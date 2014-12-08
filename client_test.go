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

	mb := NewMockBroker(t, 1)

	mb.Returns(new(MetadataResponse))

	client, err := NewClient("client_id", []string{mb.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, client)
	defer mb.Close()
}

func TestClientSeedBrokers(t *testing.T) {

	mb1 := NewMockBroker(t, 1)
	mb2 := NewMockBroker(t, 2)

	mdr := new(MetadataResponse)
	mdr.AddBroker(mb2.Addr(), mb2.BrokerID())
	mb1.Returns(mdr)

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, client)
	defer mb1.Close()
	defer mb2.Close()
}

func TestClientMetadata(t *testing.T) {

	mb1 := NewMockBroker(t, 1)
	mb5 := NewMockBroker(t, 5)
	replicas := []int32{3, 1, 5}
	isr := []int32{5, 1}

	mdr := new(MetadataResponse)
	mdr.AddBroker(mb5.Addr(), mb5.BrokerID())
	mdr.AddTopicPartition("my_topic", 0, mb5.BrokerID(), replicas, isr, NoError)
	mdr.AddTopicPartition("my_topic", 1, mb5.BrokerID(), replicas, isr, LeaderNotAvailable)
	mb1.Returns(mdr)

	config := NewClientConfig()
	config.MetadataRetries = 0
	client, err := NewClient("client_id", []string{mb1.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, client)
	defer mb1.Close()
	defer mb5.Close()

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
}

func TestClientRefreshBehaviour(t *testing.T) {
	mb1 := NewMockBroker(t, 1)
	mb5 := NewMockBroker(t, 5)

	mdr := new(MetadataResponse)
	mdr.AddBroker(mb5.Addr(), mb5.BrokerID())
	mb1.Returns(mdr)

	mdr2 := new(MetadataResponse)
	mdr2.AddTopicPartition("my_topic", 0xb, mb5.BrokerID(), nil, nil, NoError)
	mb1.Returns(mdr2)

	client, err := NewClient("clientID", []string{mb1.Addr()}, nil)
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
	mb5.Close()
	mb1.Close()
	safeClose(t, client)
}

package sarama

import (
	"testing"
)

func TestSimpleClient(t *testing.T) {

	mb := NewMockBroker(t, 1)

	mb.ExpectMetadataRequest()

	client, err := NewClient("client_id", []string{mb.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer mb.Close()
}

func TestClientExtraBrokers(t *testing.T) {

	mb1 := NewMockBroker(t, 1)
	mb2 := NewMockBroker(t, 2)

	mb1.ExpectMetadataRequest().
		AddBroker(mb2)

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer mb1.Close()
	defer mb2.Close()
}

func TestClientMetadata(t *testing.T) {

	mb1 := NewMockBroker(t, 1)
	mb5 := NewMockBroker(t, 5)

	mb1.ExpectMetadataRequest().
		AddBroker(mb5).
		AddTopicPartition("my_topic", 0, mb5.BrokerID())

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
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
	} else if len(parts) != 1 || parts[0] != 0 {
		t.Error("Client returned incorrect partitions for my_topic:", parts)
	}

	tst, err := client.Leader("my_topic", 0)
	if err != nil {
		t.Error(err)
	} else if tst.ID() != 5 {
		t.Error("Leader for my_topic had incorrect ID.")
	}
}

func TestClientRefreshBehaviour(t *testing.T) {
	mb1 := NewMockBroker(t, 1)
	mb5 := NewMockBroker(t, 5)

	mb1.ExpectMetadataRequest().
		AddBroker(mb5)

	mb5.ExpectMetadataRequest().
		AddTopicPartition("my_topic", 0xb, 5)

	client, err := NewClient("clientID", []string{mb1.Addr()}, &ClientConfig{MetadataRetries: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer mb1.Close()
	defer mb5.Close()

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
}

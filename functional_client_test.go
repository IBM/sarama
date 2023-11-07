//go:build functional

package sarama

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestFuncConnectionFailure(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	FunctionalTestEnv.Proxies["kafka1"].Enabled = false
	SaveProxy(t, "kafka1")

	config := NewFunctionalTestConfig()
	config.Metadata.Retry.Max = 1

	_, err := NewClient([]string{FunctionalTestEnv.KafkaBrokerAddrs[0]}, config)
	if !errors.Is(err, ErrOutOfBrokers) {
		t.Fatal("Expected returned error to be ErrOutOfBrokers, but was: ", err)
	}
}

func TestFuncClientMetadata(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 10 * time.Millisecond
	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}

	if err := client.RefreshMetadata("unknown_topic"); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Error("Expected ErrUnknownTopicOrPartition, got", err)
	}

	if _, err := client.Leader("unknown_topic", 0); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Error("Expected ErrUnknownTopicOrPartition, got", err)
	}

	if _, err := client.Replicas("invalid/topic", 0); !errors.Is(err, ErrUnknownTopicOrPartition) && !errors.Is(err, ErrInvalidTopic) {
		t.Error("Expected ErrUnknownTopicOrPartition or ErrInvalidTopic, got", err)
	}

	partitions, err := client.Partitions("test.4")
	if err != nil {
		t.Error(err)
	}
	if len(partitions) != 4 {
		t.Errorf("Expected test.4 topic to have 4 partitions, found %v", partitions)
	}

	partitions, err = client.Partitions("test.1")
	if err != nil {
		t.Error(err)
	}
	if len(partitions) != 1 {
		t.Errorf("Expected test.1 topic to have 1 partitions, found %v", partitions)
	}

	safeClose(t, client)
}

func TestFuncClientCoordinator(t *testing.T) {
	checkKafkaVersion(t, "0.8.2")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, NewFunctionalTestConfig())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		broker, err := client.Coordinator(fmt.Sprintf("another_new_consumer_group_%d", i))
		if err != nil {
			t.Fatal(err)
		}

		if connected, err := broker.Connected(); !connected || err != nil {
			t.Errorf("Expected to coordinator %s broker to be properly connected.", broker.Addr())
		}
	}

	safeClose(t, client)
}

package sarama

import (
	"testing"
	"time"
)

func TestFuncConnectionFailure(t *testing.T) {
	config := NewConfig()
	config.Metadata.Retry.Max = 1

	_, err := NewClient([]string{"localhost:9000"}, config)
	if err != ErrOutOfBrokers {
		t.Fatal("Expected returned error to be ErrOutOfBrokers, but was: ", err)
	}
}

func TestFuncClientMetadata(t *testing.T) {
	checkKafkaAvailability(t)

	config := NewConfig()
	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 10 * time.Millisecond
	client, err := NewClient(kafkaBrokers, config)
	if err != nil {
		t.Fatal(err)
	}

	if err := client.RefreshMetadata("unknown_topic"); err != ErrUnknownTopicOrPartition {
		t.Error("Expected ErrUnknownTopicOrPartition, got", err)
	}

	if _, err := client.Leader("unknown_topic", 0); err != ErrUnknownTopicOrPartition {
		t.Error("Expected ErrUnknownTopicOrPartition, got", err)
	}

	if _, err := client.Replicas("invalid/topic", 0); err != ErrUnknownTopicOrPartition {
		t.Error("Expected ErrUnknownTopicOrPartition, got", err)
	}

	partitions, err := client.Partitions("multi_partition")
	if err != nil {
		t.Error(err)
	}
	if len(partitions) != 2 {
		t.Errorf("Expected multi_partition topic to have 2 partitions, found %v", partitions)
	}

	partitions, err = client.Partitions("single_partition")
	if err != nil {
		t.Error(err)
	}
	if len(partitions) != 1 {
		t.Errorf("Expected single_partition topic to have 1 partitions, found %v", partitions)
	}

	safeClose(t, client)
}

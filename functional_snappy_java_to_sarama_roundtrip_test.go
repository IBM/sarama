//go:build functional

package sarama

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestJavaProducerSnappyRoundTrip tests that messages produced by Kafka's Java
// console producer with snappy compression can be correctly consumed and decompressed
// by Sarama. This verifies compatibility between Java's snappy implementation and
// klauspost/compress/snappy/xerial.
func TestJavaProducerSnappyRoundTrip(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	topic := "test-snappy-java-roundtrip"
	brokerContainer := "kafka-1"
	// Use internal listener from within the container
	brokerAddr := "kafka-1:9091"

	// Create test topic
	config := NewFunctionalTestConfig()
	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	controller, err := client.Controller()
	if err != nil {
		t.Fatalf("Failed to get controller: %v", err)
	}
	defer controller.Close()

	// Delete topic if it exists
	deleteReq := NewDeleteTopicsRequest(config.Version, []string{topic}, time.Minute)
	deleteRes, _ := controller.DeleteTopics(deleteReq)
	_ = deleteRes // ignore errors

	// Wait a bit for topic deletion
	time.Sleep(2 * time.Second)

	// Create topic
	createReq := NewCreateTopicsRequest(config.Version, map[string]*TopicDetail{
		topic: {
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}, time.Minute, false)
	createRes, err := controller.CreateTopics(createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	if err := createRes.TopicErrors[topic]; err != nil && err.Err != ErrTopicAlreadyExists && err.Err != ErrNoError {
		t.Fatalf("Failed to create topic %s: %v", topic, err)
	}

	// Wait for topic to be ready
	time.Sleep(2 * time.Second)

	// Get Kafka version to construct the correct path
	kafkaVersion := FunctionalTestEnv.KafkaVersion
	if kafkaVersion == "" {
		kafkaVersion = os.Getenv("KAFKA_VERSION")
		if kafkaVersion == "" {
			kafkaVersion = "3.9.1" // default from docker-compose.yml
		}
	}

	// Skip this test for Kafka 1.0.2
	if kafkaVersion == "1.0.2" {
		t.Skipf("Skipping test for Kafka version %s", kafkaVersion)
	}

	// Messages to produce via Java console producer
	expectedMessages := []string{
		"Hello from Java producer with snappy compression!",
		"This is message number two.",
		"Testing snappy compression round-trip.",
		"Message four: verifying decompression works correctly.",
		"Final message: Java producer -> Sarama consumer.",
	}

	// Get the current offset before producing (should be 0 for a new topic)
	// We'll consume from OffsetOldest to ensure we get all messages
	initialOffset := OffsetOldest

	// Produce messages using Java console producer with snappy compression
	producerPath := fmt.Sprintf("/opt/kafka-%s/bin/kafka-console-producer.sh", kafkaVersion)
	cmd := exec.Command("docker", "compose", "exec", "-T", brokerContainer,
		producerPath,
		"--bootstrap-server", brokerAddr,
		"--topic", topic,
		"--compression-codec", "snappy",
	)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("Failed to get stdin pipe: %v", err)
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start producer command: %v", err)
	}

	// Write messages to stdin (each message on a new line)
	for _, msg := range expectedMessages {
		_, err := fmt.Fprintln(stdin, msg)
		if err != nil {
			stdin.Close()
			_ = cmd.Wait() // ignore error since we're already handling the write error
			t.Fatalf("Failed to write message: %v", err)
		}
	}
	stdin.Close()

	// Wait for producer to finish
	if err := cmd.Wait(); err != nil {
		t.Fatalf("Producer command failed: %v", err)
	}

	t.Logf("Produced %d messages via Java console producer with snappy compression", len(expectedMessages))

	// Give Kafka time to commit and make messages available
	time.Sleep(2 * time.Second)

	// Now consume messages using Sarama
	consumer, err := NewConsumer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, initialOffset)
	if err != nil {
		t.Fatalf("Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	// Consume messages and verify they match
	var consumedMessages []string
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < len(expectedMessages); i++ {
		select {
		case msg := <-partitionConsumer.Messages():
			if msg == nil {
				t.Fatal("Received nil message")
			}
			consumedMessages = append(consumedMessages, string(msg.Value))
			t.Logf("Consumed message %d: offset=%d, value=%s", i+1, msg.Offset, string(msg.Value))

		case err := <-partitionConsumer.Errors():
			if err != nil {
				t.Fatalf("Consumer error: %v", err)
			}

		case <-ctx.Done():
			t.Fatalf("Timeout waiting for message %d", i+1)
		}
	}

	// Verify all messages were consumed
	if len(consumedMessages) != len(expectedMessages) {
		t.Fatalf("Expected %d messages, got %d", len(expectedMessages), len(consumedMessages))
	}

	// Verify each message matches exactly
	for i, expected := range expectedMessages {
		if i >= len(consumedMessages) {
			t.Fatalf("Missing message at index %d", i)
		}
		// Trim any trailing newline from consumed message
		consumed := strings.TrimRight(consumedMessages[i], "\n\r")
		if consumed != expected {
			t.Errorf("Message %d mismatch:\n  Expected: %q\n  Got:      %q", i+1, expected, consumed)
		}
	}

	t.Logf("Successfully verified round-trip: Java producer (snappy) -> Sarama consumer")
}

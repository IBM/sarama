//go:build functional

package sarama

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestJavaConsumerSnappyRoundTrip tests that messages produced by Sarama
// with snappy compression can be correctly consumed and decompressed by Kafka's Java
// console consumer. This verifies compatibility between klauspost/compress/snappy/xerial
// and Java's snappy implementation.
func TestJavaConsumerSnappyRoundTrip(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	topic := "test-snappy-sarama-to-java"
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

	// Messages to produce via Sarama with snappy compression
	expectedMessages := []string{
		"Hello from Sarama producer with snappy compression!",
		"This is message number two from Sarama.",
		"Testing snappy compression compatibility with Java consumer.",
		"Message four: verifying Sarama -> Java round-trip works correctly.",
		"Final message: Sarama producer -> Java consumer with snappy!",
	}

	// Produce messages using Sarama with snappy compression
	producerConfig := NewFunctionalTestConfig()
	producerConfig.Producer.Compression = CompressionSnappy
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.RequiredAcks = WaitForAll

	producer, err := NewSyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, producerConfig)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	for i, msgText := range expectedMessages {
		message := &ProducerMessage{
			Topic: topic,
			Key:   StringEncoder(fmt.Sprintf("key-%d", i+1)),
			Value: StringEncoder(msgText),
		}

		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			t.Fatalf("Failed to send message %d: %v", i+1, err)
		}
		t.Logf("Produced message %d: partition=%d, offset=%d, value=%s", i+1, partition, offset, msgText)
	}

	t.Logf("Produced %d messages via Sarama with snappy compression", len(expectedMessages))

	// Give Kafka time to commit and make messages available
	time.Sleep(2 * time.Second)

	// Consume messages using Java console consumer
	// Use --from-beginning to start from the earliest offset, and --max-messages to limit
	consumerPath := fmt.Sprintf("/opt/kafka-%s/bin/kafka-console-consumer.sh", kafkaVersion)
	cmd := exec.Command("docker", "compose", "exec", "-T", brokerContainer,
		consumerPath,
		"--bootstrap-server", brokerAddr,
		"--topic", topic,
		"--from-beginning",
		"--max-messages", fmt.Sprintf("%d", len(expectedMessages)),
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("Failed to get stdout pipe: %v", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("Failed to get stderr pipe: %v", err)
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start consumer command: %v", err)
	}

	// Read consumed messages from stdout
	var consumedMessages []string
	scanner := bufio.NewScanner(stdout)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := make(chan bool)
	go func() {
		for scanner.Scan() {
			line := strings.TrimRight(scanner.Text(), "\n\r")
			if line != "" {
				consumedMessages = append(consumedMessages, line)
				t.Logf("Consumed message %d: %s", len(consumedMessages), line)
			}
			if len(consumedMessages) >= len(expectedMessages) {
				break
			}
		}
		done <- true
	}()

	// Also read stderr to see any errors
	var stderrOutput strings.Builder
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			stderrOutput.WriteString(scanner.Text() + "\n")
		}
	}()

	// Wait for consumption to complete or timeout
	select {
	case <-done:
		// Messages consumed
	case <-ctx.Done():
		t.Fatalf("Timeout waiting for messages to be consumed")
	}

	// Wait for command to finish
	if err := cmd.Wait(); err != nil {
		// Check if we got the expected number of messages despite the error
		if len(consumedMessages) < len(expectedMessages) {
			t.Logf("Consumer stderr: %s", stderrOutput.String())
			t.Fatalf("Consumer command failed: %v (consumed %d/%d messages)", err, len(consumedMessages), len(expectedMessages))
		}
		// If we got all messages, the error might be expected (e.g., max-messages reached)
	}

	// Verify all messages were consumed
	if len(consumedMessages) != len(expectedMessages) {
		t.Logf("Consumer stderr: %s", stderrOutput.String())
		t.Fatalf("Expected %d messages, got %d", len(expectedMessages), len(consumedMessages))
	}

	// Verify each message matches exactly
	for i, expected := range expectedMessages {
		if i >= len(consumedMessages) {
			t.Fatalf("Missing message at index %d", i)
		}
		consumed := strings.TrimRight(consumedMessages[i], "\n\r")
		if consumed != expected {
			t.Errorf("Message %d mismatch:\n  Expected: %q\n  Got:      %q", i+1, expected, consumed)
		}
	}

	t.Logf("Successfully verified round-trip: Sarama producer (snappy) -> Java consumer")
}

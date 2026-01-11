//go:build functional

package sarama

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	brokerContainer = "kafka-1"
	brokerAddr      = "kafka-1:9091"
)

// verifyMessages verifies that consumed messages match expected messages.
func verifyMessages(t *testing.T, expectedMessages, consumedMessages []string) {
	if len(consumedMessages) != len(expectedMessages) {
		t.Fatalf("Expected %d messages, got %d", len(expectedMessages), len(consumedMessages))
	}

	for i, expected := range expectedMessages {
		if i >= len(consumedMessages) {
			t.Fatalf("Missing message at index %d", i)
		}
		consumed := strings.TrimRight(consumedMessages[i], "\n\r")
		if consumed != expected {
			t.Errorf("Message %d mismatch:\n  Expected: %q\n  Got:      %q", i+1, expected, consumed)
		}
	}
}

// TestJavaProducerSnappyRoundTrip tests that messages produced by Kafka's Java
// console producer with snappy compression can be correctly consumed and decompressed
// by Sarama. This verifies compatibility between Java's snappy implementation and
// klauspost/compress/snappy/xerial.
func TestJavaProducerSnappyRoundTrip(t *testing.T) {
	checkKafkaVersion(t, "2.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	topic := "test.1"

	expectedMessages := []string{
		"Hello from Java producer with snappy compression!",
		"This is message number two.",
		"Testing snappy compression round-trip.",
		"Message four: verifying decompression works correctly.",
		"Final message: Java producer -> Sarama consumer.",
	}

	producerPath := fmt.Sprintf("/opt/kafka-%s/bin/kafka-console-producer.sh", FunctionalTestEnv.KafkaVersion)
	cmd := exec.Command("docker", "compose", "exec", "-T", brokerContainer,
		producerPath,
		"--bootstrap-server", brokerAddr,
		"--topic", topic,
		"--compression-codec", "snappy",
	)

	stdin, err := cmd.StdinPipe()
	require.NoError(t, err, "Failed to get stdin pipe")

	err = cmd.Start()
	require.NoError(t, err, "Failed to start producer command")

	for _, msg := range expectedMessages {
		_, err := fmt.Fprintln(stdin, msg)
		if err != nil {
			stdin.Close()
			_ = cmd.Wait()
			require.NoError(t, err, "Failed to write message")
		}
	}
	stdin.Close()

	err = cmd.Wait()
	require.NoError(t, err, "Producer command failed")

	t.Logf("Produced %d messages via Java console producer with snappy compression", len(expectedMessages))

	config := NewFunctionalTestConfig()
	consumer, err := NewConsumer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err, "Failed to create consumer")
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, OffsetOldest)
	require.NoError(t, err, "Failed to create partition consumer")
	defer partitionConsumer.Close()

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
			require.NoError(t, err, "Consumer error")

		case <-ctx.Done():
			t.Fatalf("Timeout waiting for message %d", i+1)
		}
	}

	verifyMessages(t, expectedMessages, consumedMessages)
	t.Logf("Successfully verified round-trip: Java producer (snappy) -> Sarama consumer")
}

// TestJavaConsumerSnappyRoundTrip tests that messages produced by Sarama
// with snappy compression can be correctly consumed and decompressed by Kafka's Java
// console consumer. This verifies compatibility between klauspost/compress/snappy/xerial
// and Java's snappy implementation.
func TestJavaConsumerSnappyRoundTrip(t *testing.T) {
	checkKafkaVersion(t, "2.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	topic := "test.1"

	expectedMessages := []string{
		"Hello from Sarama producer with snappy compression!",
		"This is message number two from Sarama.",
		"Testing snappy compression compatibility with Java consumer.",
		"Message four: verifying Sarama -> Java round-trip works correctly.",
		"Final message: Sarama producer -> Java consumer with snappy!",
	}

	producerConfig := NewFunctionalTestConfig()
	producerConfig.Producer.Compression = CompressionSnappy
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.RequiredAcks = WaitForAll

	producer, err := NewSyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, producerConfig)
	require.NoError(t, err, "Failed to create producer")
	defer producer.Close()

	for i, msgText := range expectedMessages {
		message := &ProducerMessage{
			Topic: topic,
			Key:   StringEncoder(fmt.Sprintf("key-%d", i+1)),
			Value: StringEncoder(msgText),
		}

		partition, offset, err := producer.SendMessage(message)
		require.NoError(t, err, "Failed to send message %d", i+1)
		t.Logf("Produced message %d: partition=%d, offset=%d, value=%s", i+1, partition, offset, msgText)
	}

	t.Logf("Produced %d messages via Sarama with snappy compression", len(expectedMessages))

	consumerPath := fmt.Sprintf("/opt/kafka-%s/bin/kafka-console-consumer.sh", FunctionalTestEnv.KafkaVersion)
	cmd := exec.Command("docker", "compose", "exec", "-T", brokerContainer,
		consumerPath,
		"--bootstrap-server", brokerAddr,
		"--topic", topic,
		"--from-beginning",
		"--max-messages", fmt.Sprintf("%d", len(expectedMessages)),
	)

	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err, "Failed to get stdout pipe")

	stderr, err := cmd.StderrPipe()
	require.NoError(t, err, "Failed to get stderr pipe")

	err = cmd.Start()
	require.NoError(t, err, "Failed to start consumer command")

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

	var stderrOutput strings.Builder
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			stderrOutput.WriteString(scanner.Text() + "\n")
		}
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("Timeout waiting for messages to be consumed")
	}

	if err := cmd.Wait(); err != nil {
		if len(consumedMessages) < len(expectedMessages) {
			t.Logf("Consumer stderr: %s", stderrOutput.String())
			t.Fatalf("Consumer command failed: %v (consumed %d/%d messages)", err, len(consumedMessages), len(expectedMessages))
		}
	}

	verifyMessages(t, expectedMessages, consumedMessages)
	t.Logf("Successfully verified round-trip: Sarama producer (snappy) -> Java consumer")
}

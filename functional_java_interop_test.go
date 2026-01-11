//go:build functional

package sarama

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	brokerContainer = "kafka-1"
	brokerAddr      = "kafka-1:9091"
	zookeeperAddr   = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181"
)

var compressionTests = []struct {
	codec    CompressionCodec
	minKafka string
}{
	{CompressionNone, "0.8.0"},
	{CompressionGZIP, "0.8.0"},
	{CompressionSnappy, "0.8.0"},
	{CompressionLZ4, "0.10.0"},
	{CompressionZSTD, "2.1.0"},
}

func produceWithJava(t *testing.T, topic string, codec CompressionCodec, messages []string) {
	t.Helper()
	producerPath := fmt.Sprintf("/opt/kafka-%s/bin/kafka-console-producer.sh", FunctionalTestEnv.KafkaVersion)
	args := append(
		[]string{"compose", "exec", "-T", brokerContainer, producerPath},
		javaProducerArgs(topic, codec)...,
	)
	cmd := exec.Command("docker", args...)

	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)

	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)

	var stderrOutput strings.Builder
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s := bufio.NewScanner(stderr)
		for s.Scan() {
			stderrOutput.WriteString(s.Text() + "\n")
		}
	}()

	require.NoError(t, cmd.Start())

	for _, msg := range messages {
		_, err := fmt.Fprintln(stdin, msg)
		if err != nil {
			stdin.Close()
			waitErr := cmd.Wait()
			wg.Wait()
			if waitErr != nil {
				err = fmt.Errorf("failed to write message: %w; Java producer failed: %w; stderr: %s", err, waitErr, stderrOutput.String())
			}
		}
		require.NoError(t, err)
	}
	stdin.Close()

	err = cmd.Wait()
	wg.Wait()
	if err != nil {
		t.Logf("Java producer stderr: %s", stderrOutput.String())
		require.NoError(t, err, "Java producer failed")
	}
}

func consumeWithSarama(t *testing.T, topic string, startOffset int64, count int) []string {
	t.Helper()
	config := NewFunctionalTestConfig()
	consumer, err := NewConsumer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, startOffset)
	require.NoError(t, err)
	defer partitionConsumer.Close()

	var messages []string
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < count; i++ {
		select {
		case msg := <-partitionConsumer.Messages():
			require.NotNil(t, msg)
			messages = append(messages, string(msg.Value))
		case err := <-partitionConsumer.Errors():
			require.NoError(t, err)
		case <-ctx.Done():
			require.Fail(t, "timeout waiting for messages")
		}
	}
	return messages
}

func produceWithSarama(t *testing.T, topic string, codec CompressionCodec, messages []string) {
	t.Helper()
	config := NewFunctionalTestConfig()
	config.Producer.Compression = codec
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = WaitForAll

	producer, err := NewSyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer producer.Close()

	for _, msgText := range messages {
		_, _, err := producer.SendMessage(&ProducerMessage{
			Topic: topic,
			Value: StringEncoder(msgText),
		})
		require.NoError(t, err)
	}
}

func consumeWithJava(t *testing.T, topic string, startOffset int64, count int) []string {
	t.Helper()
	consumerPath := fmt.Sprintf("/opt/kafka-%s/bin/kafka-console-consumer.sh", FunctionalTestEnv.KafkaVersion)
	args := append(
		[]string{"compose", "exec", "-T", brokerContainer, consumerPath},
		javaConsumerArgs(topic, startOffset, count)...,
	)
	cmd := exec.Command("docker", args...)

	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)

	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)

	require.NoError(t, cmd.Start())

	var messages []string
	scanner := bufio.NewScanner(stdout)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := make(chan struct{})
	stdoutErrCh := make(chan error, 1)
	stderrErrCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for scanner.Scan() {
			if line := strings.TrimSpace(scanner.Text()); line != "" {
				messages = append(messages, line)
			}
			if len(messages) >= count {
				break
			}
		}
		stdoutErrCh <- scanner.Err()
		close(done)
	}()

	var stderrOutput strings.Builder
	go func() {
		defer wg.Done()
		s := bufio.NewScanner(stderr)
		for s.Scan() {
			stderrOutput.WriteString(s.Text() + "\n")
		}
		stderrErrCh <- s.Err()
	}()

	select {
	case <-done:
	case <-ctx.Done():
		require.Fail(t, "timeout waiting for Java consumer")
		_ = cmd.Process.Kill()
	}

	if err := cmd.Wait(); err != nil && len(messages) < count {
		t.Logf("stderr: %s", stderrOutput.String())
		require.NoError(t, err, "Java consumer failed")
	}
	wg.Wait()
	require.NoError(t, <-stdoutErrCh)
	require.NoError(t, <-stderrErrCh)
	return messages
}

func endOffsetForPartition(t *testing.T, topic string, partition int32) int64 {
	t.Helper()
	config := NewFunctionalTestConfig()
	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer safeClose(t, client)

	offset, err := client.GetOffset(topic, partition, OffsetNewest)
	require.NoError(t, err)
	return offset
}

// TestJavaProducerCompressionRoundTrip tests that messages produced by Kafka's Java
// console producer with various compression codecs can be correctly consumed and
// decompressed by Sarama.
func TestJavaProducerCompressionRoundTrip(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	for _, tc := range compressionTests {
		t.Run(tc.codec.String(), func(t *testing.T) {
			checkKafkaVersion(t, tc.minKafka)

			expected := []string{
				fmt.Sprintf("Message 1 with %s compression", tc.codec),
				"Message 2",
				"Message 3",
			}

			initialOffset := endOffsetForPartition(t, "test.1", 0)
			produceWithJava(t, "test.1", tc.codec, expected)
			actual := consumeWithSarama(t, "test.1", initialOffset, len(expected))

			require.Equal(t, expected, actual)
		})
	}
}

// TestJavaConsumerCompressionRoundTrip tests that messages produced by Sarama
// with various compression codecs can be correctly consumed and decompressed
// by Kafka's Java console consumer.
func TestJavaConsumerCompressionRoundTrip(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	for _, tc := range compressionTests {
		t.Run(tc.codec.String(), func(t *testing.T) {
			checkKafkaVersion(t, tc.minKafka)

			expected := []string{
				fmt.Sprintf("Message 1 with %s compression", tc.codec),
				"Message 2",
				"Message 3",
			}

			initialOffset := endOffsetForPartition(t, "test.1", 0)
			produceWithSarama(t, "test.1", tc.codec, expected)
			actual := consumeWithJava(t, "test.1", initialOffset, len(expected))

			require.Equal(t, expected, actual)
		})
	}
}

func kafkaVersionAtLeast(requiredVersion string) bool {
	kafkaVersion := FunctionalTestEnv.KafkaVersion
	if kafkaVersion == "" {
		return false
	}
	return parseKafkaVersion(kafkaVersion).satisfies(parseKafkaVersion(requiredVersion))
}

func javaProducerArgs(topic string, codec CompressionCodec) []string {
	args := make([]string, 0, 8)
	if kafkaVersionAtLeast("2.5.0") {
		args = append(args, "--bootstrap-server", brokerAddr)
	} else {
		args = append(args, "--broker-list", brokerAddr)
	}
	args = append(args, "--topic", topic)
	return append(args, javaProducerCompressionArgs(codec)...)
}

func javaProducerCompressionArgs(codec CompressionCodec) []string {
	if kafkaVersionAtLeast("0.10.0") {
		return []string{"--producer-property", fmt.Sprintf("compression.type=%s", codec.String())}
	}
	return []string{"--compression-codec", codec.String()}
}

func javaConsumerArgs(topic string, startOffset int64, count int) []string {
	args := make([]string, 0, 12)
	if kafkaVersionAtLeast("0.10.0") {
		args = append(args, "--bootstrap-server", brokerAddr)
	} else {
		args = append(args, "--zookeeper", zookeeperAddr)
	}
	return append(args,
		"--topic", topic,
		"--partition", "0",
		"--offset", fmt.Sprint(startOffset),
		"--max-messages", fmt.Sprint(count),
	)
}

package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	metrics "github.com/rcrowley/go-metrics"
)

var (
	messageLoad = flag.Int(
		"message-load",
		0,
		"REQUIRED: The number of messages to produce to -topic.",
	)
	messageSize = flag.Int(
		"message-size",
		0,
		"REQUIRED: The approximate size (in bytes) of each message to produce to -topic.",
	)
	brokers = flag.String(
		"brokers",
		"",
		"REQUIRED: A comma separated list of broker addresses.",
	)
	topic = flag.String(
		"topic",
		"",
		"REQUIRED: The topic to run the performance test on.",
	)
	partition = flag.Int(
		"partition",
		-1,
		"The partition of -topic to run the performance test on.",
	)
	throughput = flag.Int(
		"throughput",
		0,
		"The maximum number of messages to send per second (0 for no limit).",
	)
	maxMessageBytes = flag.Int(
		"max-message-bytes",
		1000000,
		"The max permitted size of a message.",
	)
	requiredAcks = flag.Int(
		"required-acks",
		1,
		"The required number of acks needed from the broker (-1: all, 0: none, 1: local).",
	)
	timeout = flag.Duration(
		"timeout",
		10*time.Second,
		"The duration the producer will wait to receive -required-acks.",
	)
	partitioner = flag.String(
		"partitioner",
		"roundrobin",
		"The partitioning scheme to use (hash, manual, random, roundrobin).",
	)
	compression = flag.String(
		"compression",
		"none",
		"The compression method to use (none, gzip, snappy, lz4).",
	)
	flushFrequency = flag.Duration(
		"flush-frequency",
		0,
		"The best-effort frequency of flushes.",
	)
	flushBytes = flag.Int(
		"flush-bytes",
		0,
		"The best-effort number of bytes needed to trigger a flush.",
	)
	flushMessages = flag.Int(
		"flush-messages",
		0,
		"The best-effort number of messages needed to trigger a flush.",
	)
	flushMaxMessages = flag.Int(
		"flush-max-messages",
		0,
		"The maximum number of messages the producer will send in a single request.",
	)
	retryMax = flag.Int(
		"retry-max",
		3,
		"The total number of times to retry sending a message.",
	)
	retryBackoff = flag.Duration(
		"retry-backoff",
		100*time.Millisecond,
		"The duration the producer will wait for the cluster to settle between retries.",
	)
	clientID = flag.String(
		"client-id",
		"sarama",
		"The client ID sent with every request to the brokers.",
	)
	channelBufferSize = flag.Int(
		"channel-buffer-size",
		256,
		"The number of events to buffer in internal and external channels.",
	)
	version = flag.String(
		"version",
		"0.8.2.0",
		"The assumed version of Kafka.",
	)
)

func parseCompression(scheme string) sarama.CompressionCodec {
	switch scheme {
	case "none":
		return sarama.CompressionNone
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	case "lz4":
		return sarama.CompressionLZ4
	default:
		printUsageErrorAndExit(fmt.Sprintf("Unknown -compression: %s", scheme))
	}
	panic("should not happen")
}

func parsePartitioner(scheme string, partition int) sarama.PartitionerConstructor {
	if partition < 0 && scheme == "manual" {
		printUsageErrorAndExit("-partition must not be -1 for -partitioning=manual")
	}
	switch scheme {
	case "manual":
		return sarama.NewManualPartitioner
	case "hash":
		return sarama.NewHashPartitioner
	case "random":
		return sarama.NewRandomPartitioner
	case "roundrobin":
		return sarama.NewRoundRobinPartitioner
	default:
		printUsageErrorAndExit(fmt.Sprintf("Unknown -partitioning: %s", scheme))
	}
	panic("should not happen")
}

func parseVersion(version string) sarama.KafkaVersion {
	result, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		printUsageErrorAndExit(fmt.Sprintf("unknown -version: %s", version))
	}
	return result
}

func main() {
	flag.Parse()

	if *brokers == "" {
		printUsageErrorAndExit("-brokers is required")
	}
	if *topic == "" {
		printUsageErrorAndExit("-topic is required")
	}
	if *messageLoad <= 0 {
		printUsageErrorAndExit("-message-load must be greater than 0")
	}
	if *messageSize <= 0 {
		printUsageErrorAndExit("-message-size must be greater than 0")
	}

	config := sarama.NewConfig()

	config.Producer.MaxMessageBytes = *maxMessageBytes
	config.Producer.RequiredAcks = sarama.RequiredAcks(*requiredAcks)
	config.Producer.Timeout = *timeout
	config.Producer.Partitioner = parsePartitioner(*partitioner, *partition)
	config.Producer.Compression = parseCompression(*compression)
	config.Producer.Flush.Frequency = *flushFrequency
	config.Producer.Flush.Bytes = *flushBytes
	config.Producer.Flush.Messages = *flushMessages
	config.Producer.Flush.MaxMessages = *flushMaxMessages
	config.Producer.Return.Successes = true
	config.ClientID = *clientID
	config.ChannelBufferSize = *channelBufferSize
	config.Version = parseVersion(*version)

	if err := config.Validate(); err != nil {
		printErrorAndExit(69, "Invalid configuration: %s", err)
	}

	// The async producer provides maximum performance tuning control.
	producer, err := sarama.NewAsyncProducer(strings.Split(*brokers, ","), config)
	if err != nil {
		printErrorAndExit(69, "Failed to create producer: %s", err)
	}
	defer producer.Close()

	// Construct -messageLoad messages of appoximately -messageSize random bytes.
	messages := make([]*sarama.ProducerMessage, *messageLoad)
	for i := 0; i < *messageLoad; i++ {
		payload := make([]byte, *messageSize)
		if _, err = rand.Read(payload); err != nil {
			printErrorAndExit(69, "Failed to generate message payload: %s", err)
		}
		messages[i] = &sarama.ProducerMessage{
			Topic: *topic,
			Value: sarama.ByteEncoder(payload),
		}
	}

	// Wait until all messages have been successfully sent (or an error occurs).
	done := make(chan struct{})
	go func() {
		for i := 0; i < *messageLoad; i++ {
			select {
			case <-producer.Successes():
			case err = <-producer.Errors():
				printErrorAndExit(69, "%s", err)
			}
		}
		done <- struct{}{}
	}()

	// Produce messages at approximately -throughput messages per second.
	if *throughput > 0 {
		ticker := time.NewTicker(time.Second)
		for _, message := range messages {
			for i := 0; i < *throughput; i++ {
				producer.Input() <- message
			}
			<-ticker.C
		}
		ticker.Stop()
	} else {
		for _, message := range messages {
			producer.Input() <- message
		}
	}

	<-done
	close(done)

	// TODO: Decide on a better format (or add a flag for options).
	metrics.WriteOnce(config.MetricRegistry, os.Stdout)
}

func printUsageErrorAndExit(message string) {
	fmt.Fprintln(os.Stderr, "ERROR:", message)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

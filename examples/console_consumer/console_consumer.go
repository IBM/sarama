package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

const (
	offsetEarliest      = "earliest"
	offsetLatest        = "latest"
	retryBackoffDefault = 2 * time.Second
)

var (
	brokers   = flag.String("brokers", ":9092", "The Kafka brokers to connect to, as a comma separated list.")
	topic     = flag.String("topic", "", `The topic id to consume on. (default "")`)
	partition = flag.Int("partition", 0, "The partition to consume from. (default 0)")
	clientID  = flag.String("client.id", "console_consumer", "A user-provided string sent with every request to the brokers for logging, debugging, and auditing purposes")
	offset    = flag.String("offset", offsetLatest,
		`The offset id to consume from (a non-negative number), or 'earliest' which means from beginning, or'latest' which means from end.`)
	retryBackoff         = flag.Duration("retry.backoff.ms", retryBackoffDefault, "How long to wait after a failing to read from a partition before trying again.")
	fetchMinBytes        = flag.Int("fetch.min.bytes", 1, "The minimum number of message bytes to fetch in a request.")
	fetchMessageMaxBytes = flag.Int("fetch.message.max.bytes", 0, "The maximum number of message bytes to fetch from the broker in a single request. 0 = no limits.")
)

type consumerConstructor func(addrs []string, config *sarama.Config) (sarama.Consumer, error)

var defaultConsumerConstructor = sarama.NewConsumer

type consoleLogger interface {
	Logf(format string, v ...interface{})
}

type consoleConsumerLogger struct {
	consoleLogger
}

var defaultConsoleConsumerLogger = &consoleConsumerLogger{}

func (logger *consoleConsumerLogger) Logf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func main() {
	flag.Parse()

	// Create Consumer
	consumer, err := newConsumer(parseBrokerList(*brokers), newConfig())
	if err != nil {
		log.Fatalf("unable to connect to kafka brokers %s: %s", *brokers, err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalf("error close master consumer: %s", err)
		}
	}()

	// Create Partition Consumer
	startingOffset, err := parseOffset(*offset)
	if err != nil {
		log.Fatalf("unable to parse offset: %s\n", err)
	}
	partitionConsumer, err := consumer.ConsumePartition(*topic, int32(*partition), startingOffset)
	if err != nil {
		log.Fatalf("unable to create consumer for topic (%s): %s", *topic, err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalf("error close consumer: %s", err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	log.Printf("Getting messages from Topic (%s)\n", *topic)
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			displayMessage(msg)
		case errors := <-partitionConsumer.Errors():
			displayErrors(errors)
			os.Exit(1)
		case <-signals:
			fmt.Println("Quitting...")
			os.Exit(0)
		}
	}
}

func displayMessage(msg *sarama.ConsumerMessage) {
	defaultConsoleConsumerLogger.Logf("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s\n",
		msg.Topic, msg.Partition, msg.Offset, msg.Key, string(msg.Value))
}

func displayErrors(errors *sarama.ConsumerError) {
	defaultConsoleConsumerLogger.Logf("errors: %v\n", errors)
}

// parseBrokerList parses broker list from command line arguments.
func parseBrokerList(brokersTogether string) []string {
	return strings.Split(brokersTogether, ",")
}

// parseOffset parses offset from command line arguments.
func parseOffset(offset string) (int64, error) {
	switch offset {
	case offsetLatest:
		return sarama.OffsetOldest, nil
	case offsetEarliest:
		return sarama.OffsetNewest, nil
	default:
		return strconv.ParseInt(offset, 10, 64)
	}
}

// newConfig creates and configures a *sarama.Config
// using the provided props.
func newConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.ClientID = *clientID
	config.Consumer.Retry.Backoff = *retryBackoff
	config.Consumer.Fetch.Min = int32(*fetchMinBytes)
	config.Consumer.Fetch.Max = int32(*fetchMessageMaxBytes)

	return config
}

// newConsumer creates consumer based on the provided broker list and config.
func newConsumer(brokerList []string, config *sarama.Config) (sarama.Consumer, error) {
	return consumerBuilder(brokerList, config, defaultConsumerConstructor)
}

// consumerBuilder creates consumer based on the provided broker list and config using consumerConstructor.
func consumerBuilder(brokerList []string, config *sarama.Config, constructor consumerConstructor) (sarama.Consumer, error) {
	return constructor(brokerList, config)
}

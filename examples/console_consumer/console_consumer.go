package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
)

const (
	offsetEarliest = "earliest"
	offsetLatest   = "latest"
)

var (
	brokers   = flag.String("brokers", ":9092", "The Kafka brokers to connect to, as a comma separated list.")
	topic     = flag.String("topic", "", `The topic id to consume on. (default "")`)
	partition = flag.Int("partition", 0, "The partition to consume from. (default 0)")
	offset    = flag.String("offset", offsetLatest,
		`The offset id to consume from (a non-negative number), or 'earliest' which means from beginning, or'latest' which means from end.`)
)

func main() {

	flag.Parse()

	// Create Consumer
	configProps := collectConfigProps()
	consumer, err := newConsumer(parseBrokerList(*brokers), newConfig(configProps))
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

	fmt.Printf("Getting messages from Topic:%s\n", *topic)
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s\n",
				msg.Topic, msg.Partition, msg.Offset, msg.Key, string(msg.Value))
		case errors := <-partitionConsumer.Errors():
			log.Fatalf("errors: %v\n", errors)
		case <-signals:
			fmt.Println("Quitting...")
			os.Exit(0)

		}
	}

}

// collectConfigProps parses broker list from command line arguments.
func collectConfigProps() map[string]string {
	props := make(map[string]string)

	return props
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
func newConfig(props map[string]string) *sarama.Config {

	config := sarama.NewConfig()

	return config
}

// newConsumer creates consumer based on the provided config.
func newConsumer(brokerList []string, config *sarama.Config) (sarama.Consumer, error) {
	c, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		return nil, err
	}

	return c, nil
}

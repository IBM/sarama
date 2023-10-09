package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/IBM/sarama"
)

var (
	brokerList = "localhost:9092"
	topic      = "test-topic"
	logger     = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {

	sarama.Logger = logger
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewHashPartitioner
	message := &sarama.ProducerMessage{Topic: topic, Partition: int32(-1)}

	producer, err := sarama.NewSyncProducer(strings.Split(brokerList, ","), config)
	if err != nil {
		printErrorAndExit(69, "Failed to open Kafka producer: %s", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Println("Failed to close Kafka producer cleanly:", err)
		}
	}()

	fmt.Println("Input message content:")
	for {
		fmt.Print(">")
		input := bufio.NewScanner(os.Stdin)
		input.Scan()
		message.Value = sarama.StringEncoder(input.Text())
		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			printErrorAndExit(69, "Failed to produce message: %s", err)
		} else {
			fmt.Printf("topic=%s\tpartition=%d\toffset=%d\n", topic, partition, offset)
		}
	}
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

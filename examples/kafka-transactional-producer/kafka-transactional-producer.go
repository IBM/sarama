package main

import (
	"flag"
	"fmt"
	"github.com/rcrowley/go-metrics"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

var (
	brokerList  = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster. You can also set the KAFKA_PEERS environment variable")
	topic       = flag.String("topic", "", "REQUIRED: the topic to produce to")
	key         = flag.String("key", "", "The key of the message to produce. Can be empty.")
	value       = flag.String("value", "", "REQUIRED: the value of the message to produce. You can also provide the value on stdin.")
	partitioner = flag.String("partitioner", "", "The partitioning scheme to use. Can be `hash`, `manual`, or `random`")
	partition   = flag.Int("partition", -1, "The partition to produce to.")
	verbose     = flag.Bool("verbose", false, "Turn on sarama logging to stderr")
	showMetrics = flag.Bool("metrics", false, "Output metrics on successful publish to stderr")
	silent      = flag.Bool("silent", false, "Turn off printing the message's topic, partition, and offset to stdout")
	messages    = flag.Int("messages", 5, "How many messages send")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *brokerList == "" {
		printUsageErrorAndExit("no -brokers specified. Alternatively, set the KAFKA_PEERS environment variable")
	}

	if *topic == "" {
		printUsageErrorAndExit("no -topic specified")
	}

	if *verbose {
		sarama.Logger = logger
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.TransactionalID = "transactional-id"
	config.Version = sarama.V0_11_0_0

	switch *partitioner {
	case "":
		if *partition >= 0 {
			config.Producer.Partitioner = sarama.NewManualPartitioner
		} else {
			config.Producer.Partitioner = sarama.NewHashPartitioner
		}
	case "hash":
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case "random":
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case "manual":
		config.Producer.Partitioner = sarama.NewManualPartitioner
		if *partition == -1 {
			printUsageErrorAndExit("-partition is required when partitioning manually")
		}
	default:
		printUsageErrorAndExit(fmt.Sprintf("Partitioner %s not supported.", *partitioner))
	}

	message := &sarama.ProducerMessage{Topic: *topic, Partition: int32(*partition)}

	if *key != "" {
		message.Key = sarama.StringEncoder(*key)
	}

	if *value != "" {
		message.Value = sarama.StringEncoder(*value)
	} else if stdinAvailable() {
		bytes, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			printErrorAndExit(66, "Failed to read data from the standard input: %s", err)
		}
		message.Value = sarama.ByteEncoder(bytes)
	} else {
		printUsageErrorAndExit("-value is required, or you have to provide the value on stdin")
	}

	producer, err := sarama.NewSyncProducer(strings.Split(*brokerList, ","), config)
	if err != nil {
		printErrorAndExit(69, "Failed to open Kafka producer: %s", err)
	}

	send(producer, *config, message, false)
	send(producer, *config, message, false)
	send(producer, *config, message, true)
	send(producer, *config, message, false)
	send(producer, *config, message, true)
	send(producer, *config, message, false)

	defer func() {
		if err := producer.Close(); err != nil {
			logger.Println("Failed to close Kafka producer cleanly:", err)
		}
	}()

}

func send(producer sarama.SyncProducer, config sarama.Config, message *sarama.ProducerMessage, abort bool) {
	initProducerReq := new(sarama.InitProducerIDRequest) //TODO Refactor to accept only transactionalId, or nothing at all and take all from configuration
	initProducerReq.TransactionalID = &config.Producer.TransactionalID
	initProducerReq.TransactionTimeout = time.Millisecond * 100
	if abort{
		message.Value = sarama.StringEncoder("Abort")
	}else{
		message.Value = sarama.StringEncoder("Tx")
	}
	for j := 0; j < 1; j++ {
		_, err := producer.InitializeTransactions(initProducerReq)
		if err != nil {
			printErrorAndExit(69, "Failed to initialize producerId: %s", err)
		}
		producer.BeginTransaction(topicPartitions(*message))
		for i := 0; i < 6; i++ {
			partition, offset, err := producer.SendMessage(message)
			if err != nil {
				printErrorAndExit(69, "Failed to produce message: %s", err)
			} else if !*silent {
				log.Println(fmt.Sprintf("topic=%s\tpartition=%d\toffset=%d", message.Topic, partition, offset))
			}
			if *showMetrics {
				metrics.WriteOnce(config.MetricRegistry, os.Stderr)
			}

		}
		if !abort {
			producer.CommitTransaction()
		} else {
			producer.AbortTransaction()
		}

		fmt.Println()
		time.Sleep(2 * time.Second)
	}
}

func topicPartitions(messages ...sarama.ProducerMessage) map[string][]int32 { //TODO Decide signature for BeginTransactions and then move it into producer or not
	topicPartitionsMap := make(map[string]map[int32]bool) // TODO maybe use some library to have a set structure
	for _, message := range messages {
		if _, ok := topicPartitionsMap[message.Topic]; !ok {
			topicPartitionsMap[message.Topic] = make(map[int32]bool)
		}
		partitions := topicPartitionsMap[message.Topic]
		if _, ok := partitions[message.Partition]; !ok {
			partitions[message.Partition] = true
		}
	}

	result := make(map[string][]int32)
	for k, v := range topicPartitionsMap {
		result[k] = func(arg map[int32]bool) []int32 {
			list := make([]int32, len(arg))
			i := 0
			for k, _ := range arg {
				list[i] = k
				i += 1
			}
			return list
		}(v)
	}
	return result

}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func printUsageErrorAndExit(message string) {
	fmt.Fprintln(os.Stderr, "ERROR:", message)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}

func stdinAvailable() bool {
	stat, _ := os.Stdin.Stat()
	return (stat.Mode() & os.ModeCharDevice) == 0
}

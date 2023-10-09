package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

// Sarama configuration options
var (
	brokers   = ""
	version   = ""
	group     = ""
	topics    = ""
	assignors = ""
	oldest    = true
	verbose   = false
)

func init() {
	flag.StringVar(&brokers, "brokers", "", "Kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&group, "group", "", "Kafka consumer group definition")
	flag.StringVar(&version, "version", "2.1.1", "Kafka cluster version")
	flag.StringVar(&topics, "topics", "", "Kafka topics to be consumed, as a comma separated list")
	flag.StringVar(&assignors, "assignors", "cooperative-sticky", "Consumer group partition assignment strategies (range, roundrobin, sticky, cooperative-sticky)")
	flag.BoolVar(&oldest, "oldest", true, "Kafka consumer consume initial offset from oldest")
	flag.BoolVar(&verbose, "verbose", false, "Sarama logging")

	flag.Parse()

	if len(brokers) == 0 {
		panic("no Kafka bootstrap brokers defined, please set the -brokers flag")
	}

	if len(topics) == 0 {
		panic("no topics given to be consumed, please set the -topics flag")
	}

	if len(group) == 0 {
		panic("no Kafka consumer group defined, please set the -group flag")
	}
}

func main() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags|log.Lshortfile|log.Lmsgprefix)
	log.Println("Starting a new Sarama consumer")

	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config := sarama.NewConfig()
	config.Version = version

	var strategies []sarama.BalanceStrategy
	for _, assignor := range strings.Split(assignors, " ") {
		switch assignor {
		case "sticky":
			strategies = append(strategies, sarama.NewBalanceStrategySticky())
		case "roundrobin":
			strategies = append(strategies, sarama.NewBalanceStrategyRoundRobin())
		case "range":
			strategies = append(strategies, sarama.NewBalanceStrategyRange())
		case "cooperative-sticky":
			strategies = append(strategies, sarama.NewBalanceStrategyCooperativeSticky())
		default:
			log.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
		}
	}
	config.Consumer.Group.Rebalance.GroupStrategies = strategies

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := Consumer{}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	consumptionIsPaused := false
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		var err error
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err = client.ConsumeV2(ctx, strings.Split(topics, ","), &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// return if consumer is closed
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
		}
	}()

	log.Println("Sarama consumer up and running!...")

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	keepRunning := true
	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			log.Println("terminating: via signal")
			keepRunning = false
		case <-sigusr1:
			toggleConsumptionFlow(client, &consumptionIsPaused)
		}
	}
	cancel()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
	wg.Wait()
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		client.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct{}

// Setup runs at the beginning of setting up new assigned partitions, before ConsumeClaim.
// For EAGER rebalance strategy, this is to set up all assigned partitions.
// For COOPERATIVE rebalance strategy, this is only to set up new assigned partitions.
// Note that even if there are no new assigned partitions, this method will still be called after rebalance.
func (consumer *Consumer) Setup(offsetManger sarama.OffsetManager, newAssignedPartitions map[string][]int32) {
	log.Printf("[Setup] newAssignedPartitions: %v", newAssignedPartitions)
}

// Cleanup runs after ConsumeClaim, but before the offsets are committed for the claim.
// For EAGER rebalance strategy, this is to clean up all assigned partitions.
// For COOPERATIVE rebalance strategy, this is only to clean up revoked partitions.
func (consumer *Consumer) Cleanup(offsetManger sarama.OffsetManager, revokedPartitions map[string][]int32) {
	log.Printf("[Cleanup] revokedPartitions: %v", revokedPartitions)
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once ctx is done, ConsumeClaim should return as soon as possible.
func (consumer *Consumer) ConsumeClaim(ctx context.Context, om sarama.OffsetManager, claim sarama.ConsumerGroupClaim) {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		// `<-ctx.Done()` has a higher priority than `<-claim.Messages()`
		select {
		case <-ctx.Done():
			return
		default:
		}

		select {
		// Should return immediately when `ctx.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-ctx.Done():
			return

		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return
			}
			log.Printf("received message topic:%s, partition:%d, offset:%d, value:%s", message.Topic, message.Partition, message.Offset, message.Value)
			om.MarkMessage(message, "")
		}
	}
}

package main

// Worked example: a load-aware sticky consumer that reports a fresh load
// sample (CPU%, in-flight count, lag) to the group leader on every JoinGroup
// cycle via sarama.SubscriptionUserDataProvider. See load_aware_sticky.go for
// the strategy wrapper.

import (
	"context"
	"errors"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/IBM/sarama"
)

var (
	brokers = ""
	version = ""
	group   = ""
	topics  = ""
	verbose = false
)

func init() {
	flag.StringVar(&brokers, "brokers", "", "Kafka bootstrap brokers, comma separated")
	flag.StringVar(&group, "group", "", "Kafka consumer group id")
	flag.StringVar(&version, "version", sarama.DefaultVersion.String(), "Kafka cluster version")
	flag.StringVar(&topics, "topics", "", "Kafka topics to consume, comma separated")
	flag.BoolVar(&verbose, "verbose", false, "Sarama logging")
	flag.Parse()

	if brokers == "" || topics == "" || group == "" {
		log.Panic("-brokers, -topics, and -group are all required")
	}
}

func main() {
	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	v, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = v
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer := &Consumer{ready: make(chan bool)}

	// Plug in the load-aware sticky strategy. The observer is called once per
	// JoinGroup cycle to capture a fresh sample of local load.
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		NewLoadAwareSticky(consumer.loadSample),
	}

	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, strings.Split(topics, ","), consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready
	log.Println("load-aware sticky consumer running")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

// Consumer is a minimal ConsumerGroupHandler that also tracks an in-flight
// counter so it can report a meaningful load sample.
type Consumer struct {
	ready    chan bool
	inFlight atomic.Int64
}

// loadSample is invoked by LoadAwareSticky once per JoinGroup. In a real
// deployment this would read /proc/stat, runtime.NumGoroutine, consumer lag,
// or whatever metric the leader uses to weight assignment.
func (c *Consumer) loadSample() LoadSample {
	return LoadSample{
		Version:    1,
		CPUPercent: rand.Float64() * 100,
		InFlight:   int(c.inFlight.Load()),
	}
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			c.inFlight.Add(1)
			log.Printf("Message claimed: topic=%s partition=%d offset=%d", message.Topic, message.Partition, message.Offset)
			session.MarkMessage(message, "")
			c.inFlight.Add(-1)
		case <-session.Context().Done():
			return nil
		}
	}
}

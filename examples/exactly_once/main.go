package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// Sarama configuration options
var (
	brokers          = ""
	version          = ""
	group            = ""
	topics           = ""
	destinationTopic = ""
	oldest           = true
	verbose          = false
	assignor         = ""
)

func init() {
	flag.StringVar(&brokers, "brokers", "", "Kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&group, "group", "", "Kafka consumer group definition")
	flag.StringVar(&version, "version", sarama.DefaultVersion.String(), "Kafka cluster version")
	flag.StringVar(&topics, "topics", "", "Kafka topics to be consumed, as a comma separated list")
	flag.StringVar(&destinationTopic, "destination-topic", "", "Kafka topic where records will be copied from topics.")
	flag.StringVar(&assignor, "assignor", "range", "Consumer group partition assignment strategy (range, roundrobin, sticky)")
	flag.BoolVar(&oldest, "oldest", true, "Kafka consumer consume initial offset from oldest")
	flag.BoolVar(&verbose, "verbose", false, "Sarama logging")
	flag.Parse()

	if len(brokers) == 0 {
		panic("no Kafka bootstrap brokers defined, please set the -brokers flag")
	}

	if len(topics) == 0 {
		panic("no topics given to be consumed, please set the -topics flag")
	}

	if len(destinationTopic) == 0 {
		panic("no destination topics given to be consumed, please set the -destination-topics flag")
	}

	if len(group) == 0 {
		panic("no Kafka consumer group defined, please set the -group flag")
	}
}

func main() {
	keepRunning := true
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

	switch assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	case "roundrobin":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	case "range":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
	}

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	config.Consumer.IsolationLevel = sarama.ReadCommitted
	config.Consumer.Offsets.AutoCommit.Enable = false

	producerProvider := newProducerProvider(strings.Split(brokers, ","), func() *sarama.Config {
		producerConfig := sarama.NewConfig()
		producerConfig.Version = version

		producerConfig.Net.MaxOpenRequests = 1
		producerConfig.Producer.RequiredAcks = sarama.WaitForAll
		producerConfig.Producer.Idempotent = true
		producerConfig.Producer.Transaction.ID = "sarama"
		return producerConfig
	})

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := Consumer{
		groupId:          group,
		brokers:          strings.Split(brokers, ","),
		producerProvider: producerProvider,
		ready:            make(chan bool),
	}

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
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, strings.Split(topics, ","), &consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

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
	wg.Wait()

	producerProvider.clear()

	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
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
type Consumer struct {
	ready            chan bool
	groupId          string
	brokers          []string
	producerProvider *producerProvider
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L2
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}
			func() {
				producer := consumer.producerProvider.borrow(message.Topic, message.Partition)
				defer consumer.producerProvider.release(message.Topic, message.Partition, producer)

				startTime := time.Now()

				// BeginTxn must be called before any messages.
				err := producer.BeginTxn()
				if err != nil {
					log.Printf("Message consumer: unable to start transaction: %+v", err)
					return
				}
				// Produce current record in producer transaction.
				producer.Input() <- &sarama.ProducerMessage{
					Topic: destinationTopic,
					Key:   sarama.ByteEncoder(message.Key),
					Value: sarama.ByteEncoder(message.Value),
				}

				// You can add current message to this transaction
				err = producer.AddMessageToTxn(message, consumer.groupId, nil)
				if err != nil {
					log.Println("error on AddMessageToTxn")
					consumer.handleTxnError(producer, message, session, err, func() error {
						return producer.AddMessageToTxn(message, consumer.groupId, nil)
					})
					return
				}

				// Commit producer transaction.
				err = producer.CommitTxn()
				if err != nil {
					log.Println("error on CommitTxn")
					consumer.handleTxnError(producer, message, session, err, func() error {
						return producer.CommitTxn()
					})
					return
				}
				log.Printf("Message claimed [%s]: value = %s, timestamp = %v, topic = %s, partition = %d", time.Since(startTime), string(message.Value), message.Timestamp, message.Topic, message.Partition)
			}()
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

func (consumer *Consumer) handleTxnError(producer sarama.AsyncProducer, message *sarama.ConsumerMessage, session sarama.ConsumerGroupSession, err error, defaulthandler func() error) {
	log.Printf("Message consumer: unable to process transaction: %+v", err)
	for {
		if producer.TxnStatus()&sarama.ProducerTxnFlagFatalError != 0 {
			// fatal error. need to recreate producer.
			log.Printf("Message consumer: producer is in a fatal state, need to recreate it")
			// reset current consumer offset to retry consume this record.
			session.ResetOffset(message.Topic, message.Partition, message.Offset, "")
			return
		}
		if producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
			err = producer.AbortTxn()
			if err != nil {
				log.Printf("Message consumer: unable to abort transaction: %+v", err)
				continue
			}
			// reset current consumer offset to retry consume this record.
			session.ResetOffset(message.Topic, message.Partition, message.Offset, "")
			return
		}
		// if not you can retry
		err = defaulthandler()
		if err == nil {
			return
		}
	}
}

type topicPartition struct {
	topic     string
	partition int32
}

type producerProvider struct {
	producersLock sync.Mutex
	producers     map[topicPartition][]sarama.AsyncProducer

	producerProvider func(topic string, partition int32) sarama.AsyncProducer
}

func newProducerProvider(brokers []string, producerConfigurationProvider func() *sarama.Config) *producerProvider {
	provider := &producerProvider{
		producers: make(map[topicPartition][]sarama.AsyncProducer),
	}
	provider.producerProvider = func(topic string, partition int32) sarama.AsyncProducer {
		config := producerConfigurationProvider()
		if config.Producer.Transaction.ID != "" {
			config.Producer.Transaction.ID = config.Producer.Transaction.ID + "-" + topic + "-" + fmt.Sprint(partition)
		}
		producer, err := sarama.NewAsyncProducer(brokers, config)
		if err != nil {
			return nil
		}
		return producer
	}
	return provider
}

func (p *producerProvider) borrow(topic string, partition int32) (producer sarama.AsyncProducer) {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	tp := topicPartition{topic: topic, partition: partition}

	if producers, ok := p.producers[tp]; !ok || len(producers) == 0 {
		for {
			producer = p.producerProvider(topic, partition)
			if producer != nil {
				return
			}
		}
	}

	index := len(p.producers[tp]) - 1
	producer = p.producers[tp][index]
	p.producers[tp] = p.producers[tp][:index]
	return
}

func (p *producerProvider) release(topic string, partition int32, producer sarama.AsyncProducer) {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	if producer.TxnStatus()&sarama.ProducerTxnFlagInError != 0 {
		// Try to close it
		_ = producer.Close()
		return
	}
	tp := topicPartition{topic: topic, partition: partition}
	p.producers[tp] = append(p.producers[tp], producer)
}

func (p *producerProvider) clear() {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	for _, producers := range p.producers {
		for _, producer := range producers {
			producer.Close()
		}
	}
	for _, producers := range p.producers {
		producers = producers[:0]
	}
}

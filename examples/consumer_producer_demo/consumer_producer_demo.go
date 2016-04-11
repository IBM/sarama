package main

import (
	"github.com/Shopify/sarama"

	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"
	"strconv"
	"encoding/json"
)

var (
	brokers   = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	topic     = flag.String("topic", "consumer_producer_demo", "Topic to which we are going to produce and consume from")
	verbose   = flag.Bool("verbose", false, "Turn on Sarama logging")
)

func main() {
	flag.Parse()

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Bootstrap Kafka brokers are: %s", strings.Join(brokerList, ", "))

	producer := &Producer{
		SimpleMessageProducer: newSimpleMessageProducer(brokerList, true),
		Topic: *topic,
	}
	go producer.produce()

	consumer := &SimpleConsumer{
		SimpleMessageConsumer: newSimpleMessageConsumer(brokerList),
	}

	// Get all partitions for the topic and select one of them to consume from. Note that Sarama doesn't have
	// support for coordinated consumption as of yet
	partitions, _ := consumer.GetPartitions(*topic)
	consumer.consumeFrom(*topic, partitions[0])
}

type Producer struct {
	SimpleMessageProducer sarama.AsyncProducer
	Topic string
}

func (p *Producer) produce() {
	// This loop produces 10 default messages to the specified topic
	log.Printf("Sending 10 messages...")
	for i := 0; i < 10; i++ {
		p.SimpleMessageProducer.Input() <- &sarama.ProducerMessage{
			Topic: p.Topic,
			Key:   sarama.StringEncoder("test_msg"),
			Value: &EncodedMessage{
				iteration: i,
			},
		}
	}
	log.Printf("Finished sending 10 messages")
}

type SimpleConsumer struct {
	SimpleMessageConsumer sarama.Consumer
}

func (c *SimpleConsumer) consumeFrom(topic string, partition int32) {

	log.Println("Will start consumption from the earliest offset")
	consumer, _ := c.SimpleMessageConsumer.ConsumePartition(topic, partition, 0)
	msgCount := 0

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	iAmDone := make(chan struct{})

	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
		log.Println("Successfully closed the consumer")
	}()

	// Note that this consumer would block and wait for more messages
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				log.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				log.Println("Received message:", string(msg.Key), string(msg.Value))
			case <-signals:
				log.Println("Interrupt is detected. Will close the consumer now.")
				iAmDone <- struct{}{}
			}
		}
	}()

	<- iAmDone
	log.Println("Total messages consumed: ", msgCount)
}

func (c *SimpleConsumer) GetPartitions(topic string) ([]int32, error) {
	return c.SimpleMessageConsumer.Partitions(topic)
}

type EncodedMessage struct {
	iteration int
	encoded []byte
	err     error
}

func (emsg *EncodedMessage) Encode() ([]byte, error) {
	msg, err := json.Marshal("Test message " + strconv.Itoa(emsg.iteration))
	return msg, err
}

func (emsg *EncodedMessage) Length() int {
	return len(emsg.encoded)
}

func newSimpleMessageProducer(brokerList []string, isAsync bool) sarama.AsyncProducer {

	config := sarama.NewConfig()
	if isAsync {
		config.Producer.RequiredAcks = sarama.WaitForLocal  // Only wait for the leader to ack which makes it async producer
	} else {
		config.Producer.RequiredAcks = sarama.WaitForAll  // Wait for all the brokers
	}
	config.Producer.Compression = sarama.CompressionNone   // Do not compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	// We will just log to STDOUT if we're not able to produce messages.
	// Note: messages will only be returned here after all retry attempts are exhausted.
	go func() {
		for err := range producer.Errors() {
			log.Println("Unable to produce messages. Error:", err)
		}
	}()

	return producer
}

func newSimpleMessageConsumer(brokerList []string) sarama.Consumer {
	log.Printf("Initializing new consumer")
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		log.Println(err)
		panic(err)
	}

	defer func() {
		if err != nil {
			log.Println("Received an error and will try closing the consumer. Error: ", err)
			if err := consumer.Close(); err != nil {
				log.Println(err)
				panic(err)
			}
		}
	}()

	return consumer
}

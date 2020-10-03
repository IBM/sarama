package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"go.opentelemetry.io/otel/exporters/stdout"
)

var (
	brokers = flag.String("brokers", "localhost:9092", "The Kafka brokers to connect to, as a comma separated list")
	topic   = flag.String("topic", "default_topic", "The Kafka topic to use")
	logger  = log.New(os.Stdout, "[OTelInterceptor] ", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *brokers == "" {
		logger.Fatalln("at least one broker is required")
	}
	splitBrokers := strings.Split(*brokers, ",")
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)

	// oTel stdout example
	pusher, err := stdout.InstallNewPipeline([]stdout.Option{
		stdout.WithQuantiles([]float64{0.5, 0.9, 0.99}),
	}, nil)
	if err != nil {
		logger.Fatalf("failed to initialize stdout export pipeline: %v", err)
	}
	defer pusher.Stop()

	// simple sarama producer that adds a new producer interceptor
	conf := sarama.NewConfig()
	conf.Version = sarama.V0_11_0_0
	conf.Producer.Interceptors = []sarama.ProducerInterceptor{NewOTelInterceptor(splitBrokers)}

	producer, err := sarama.NewAsyncProducer(splitBrokers, conf)
	if err != nil {
		panic("Couldn't create a Kafka producer")
	}
	defer producer.AsyncClose()

	// kill -2, trap SIGINT to trigger a shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// ticker
	bulkSize := 2
	duration := 5 * time.Second
	ticker := time.NewTicker(duration)
	logger.Printf("Starting to produce %v messages every %v", bulkSize, duration)
	for {
		select {
		case t := <-ticker.C:
			now := t.Format(time.RFC3339)
			logger.Printf("\nproducing %v messages to topic %s at %s", bulkSize, *topic, now)
			for i := 0; i < bulkSize; i++ {
				producer.Input() <- &sarama.ProducerMessage{
					Topic: *topic, Key: nil,
					Value: sarama.StringEncoder(fmt.Sprintf("test message %v/%v from kafka-client-go-test at %s", i+1, bulkSize, now)),
				}
			}
		case <-signals:
			logger.Println("terminating the program")
			logger.Println("Bye :)")
			return
		}
	}
}

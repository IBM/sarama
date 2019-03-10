package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	// when testing locally, it is way faster to edit this to reference your local repo if different
	// if you want to do it properly but slowly you may use some dependency manager magic
	"github.com/Shopify/sarama"
)

var (
	brokerList = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster")
	topic      = flag.String("topic", "", "REQUIRED: the topic to consume")
)

func main() {
	flag.Parse()

	if *brokerList == "" {
		log.Fatal("You have to provide -brokers as a comma-separated list, or set the KAFKA_PEERS environment variable.")
	}
	if *topic == "" {
		log.Fatal("-topic is required")
	}

	config := sarama.NewConfig()
	config.Version = sarama.V1_1_1_0
	config.Consumer.IsolationLevel = sarama.ReadCommitted
	config.Consumer.MaxProcessingTime = 20 * 365 * 24 * time.Hour

	c, err := sarama.NewConsumer(strings.Split(*brokerList, ","), config)
	if err != nil {
		log.Fatalf("Failed to start consumer: %s", err)
	}

	partitionList, err := c.Partitions(*topic)
	if err != nil {
		log.Fatalf("Failed to get the list of partitions: %s", err)
	}

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(*topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Fatalf("Failed to start consumer for partition %d: %s", partition, err)
		}

		go func() {
			for err := range pc.Errors() {
				log.Fatal(err)
			}
		}()

		msgChannel := pc.Messages()
	read1Partition:
		for {
			//timeout := time.NewTimer(1 * time.Second)
			select {
			case msg, open := <-msgChannel:
				if !open {
					log.Println("channel message is closed")
					break read1Partition
				}
				log.Println(string(msg.Value))
				//case <-timeout.C:
				//	break read1Partition
			}
		}
	}

	log.Println("Done consuming topic", *topic)

	if err := c.Close(); err != nil {
		log.Println("Failed to close consumer: ", err)
	}
}

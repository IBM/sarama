package main

import (
	"log"

	"github.com/Shopify/sarama"
)

var (
	brokers         = []string{"localhost:9092"}
	transactionalID = "my-consumer-0"
	topic           = "test-topic"
	commit          = true // change this to either abort or commit the message
)

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Idempotent = true
	config.Version = sarama.V1_1_1_0
	config.Net.MaxOpenRequests = 1
	config.Producer.RequiredAcks = sarama.WaitForAll

	config.Producer.TransactionalID = &transactionalID

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Panic(err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("some random message"),
	}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		log.Panic(err)
	}

	if commit {
		err = producer.CommitTxn()
		if err != nil {
			log.Panic(err)
		}
	} else {
		err = producer.AbortTxn()
		if err != nil {
			log.Panic(err)
		}
	}

}

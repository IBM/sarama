package main

import (
	"fmt"
	"github.com/shopify/sarama"
)

func main() {

	id := "example-client"
	host := "127.0.0.1"
	port := 9092

	client, err := sarama.NewClient(id, host, port)
	if err != nil {
		panic(err)
	}

	topic := "test"
	partition := 0
	group := "example-consumer-group"

	consumer, err := sarama.NewConsumer(client, topic, partition, group)
	if err != nil {
		panic(err)
	}

	fmt.Println("Listening on topic:", topic, "partition:", partition)
	for msgOrErr := range consumer.Messages() {
		if err := msgOrErr.Err; err != nil {
			fmt.Println(err)
			continue
		}
		msg := msgOrErr.Msg
		fmt.Println("Got message with key:", string(msg.Key), "value:", string(msg.Value), "offset:", msgOrErr.Offset)
	}

}

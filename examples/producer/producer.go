package main

import (
	"fmt"
	"github.com/shopify/sarama"
	"time"
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

	producer := sarama.NewProducer(client, topic, sarama.RandomPartitioner{})

	ticker := time.Tick(1 * time.Second)
	remaining := 5
	for {
		select {
		case <-ticker:

			if remaining == 0 {
				return
			}
			remaining--

			key := fmt.Sprintf("foo%d", remaining)
			value := fmt.Sprintf("bar%d", remaining)
			fmt.Println("Sending message with key:", key, "value:", value)
			err = producer.SendMessageString(key, value)
			if err != nil {
				panic(err)
			}

		}
	}
}

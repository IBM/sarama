package sarama

import (
	"fmt"
)

type exampleConsumerGroupHandler func(sess ConsumerGroupSession, claim ConsumerGroupClaim) error

func (h exampleConsumerGroupHandler) Setup(_ ConsumerGroupSession) error   { return nil }
func (h exampleConsumerGroupHandler) Cleanup(_ ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(s ConsumerGroupSession, c ConsumerGroupClaim) error {
	return h(s, c)
}

func ExampleConsumerGroup() {
	// Init config, specify version
	config := NewConfig()
	config.Version = V1_0_0_0
	config.Consumer.Return.Errors = true

	// Start with a client
	client, err := NewClient([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()

	// Start a new consumer group
	group, err := NewConsumerGroupFromClient("my-group", client)
	if err != nil {
		panic(err)
	}
	defer func() { _ = group.Close() }()

	// Track errors
	go func() {
		for err := range group.Errors() {
			fmt.Println("ERROR", err)
		}
	}()

	// Iterate over consumer sessions.
	for {
		err := group.Consume([]string{"my-topic"}, exampleConsumerGroupHandler(func(sess ConsumerGroupSession, claim ConsumerGroupClaim) error {
			for msg := range claim.Messages() {
				fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
				sess.MarkMessage(msg, "")
			}
			return nil
		}))
		if err != nil {
			panic(err)
		}
	}
}

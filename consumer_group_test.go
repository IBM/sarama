package sarama

import (
	"fmt"
	"sync"
)

func ExampleConsumerGroup() {
	// Start with a client
	client, err := NewClient([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Start a consumer manager
	consumers, err := NewConsumerFromClient(client)
	if err != nil {
		panic(err)
	}
	defer consumers.Close()

	// Start an offset manager
	offsets, err := NewOffsetManagerFromClient("my-group", client)
	if err != nil {
		panic(err)
	}
	defer offsets.Close()

	// Start a new consumer group
	group := NewConsumerGroupFromClient("my-group", client)

	// Prepare consumer process
	consumePartition := func(sess ConsumerGroupSession, topic string, partition int32) error {
		pom, err := offsets.ManagePartition(topic, partition)
		if err != nil {
			return err
		}
		defer pom.Close()

		go func() {
			for err := range pom.Errors() {
				fmt.Printf("Partition offset manager error: %v\n", err)
			}
		}()

		offset, _ := pom.NextOffset()
		pcm, err := consumers.ConsumePartition(topic, partition, offset)
		if err != nil {
			return err
		}
		defer pcm.Close()

		for {
			select {
			case msg, more := <-pcm.Messages():
				if !more {
					return nil
				}
				fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
				pom.MarkOffset(msg.Offset, "")
			case <-sess.Done(): // exit when next rebalance cycle was triggered
				return nil
			}
		}
	}

	// Iterate over consumer group sessions
	for {
		sess, err := group.Subscribe([]string{"myTopic"})
		if err != nil {
			panic(err)
		}

		func() {
			// Make sure to close the session once all
			// consumers have exited.
			defer sess.Close()

			// Consume claimed partitions in separate goroutines.
			wg := new(sync.WaitGroup)
			for topic, partitions := range sess.Claims() {
				for _, partition := range partitions {
					topic, partition := topic, partition

					wg.Add(1)
					go func() {
						defer wg.Done()

						// Signal the end of as session as soon as the first
						// consumer exits.
						defer sess.Stop()

						// Consume a single partition until the session
						// is stopped and a rebalance cycle is due.
						err := consumePartition(sess, topic, partition)
						if err != nil {
							fmt.Println("ERROR", err)
						}
					}()
				}
			}
			wg.Wait()
		}()
		if err != nil {
			panic(err)
		}
	}
}

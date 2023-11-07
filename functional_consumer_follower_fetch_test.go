//go:build functional

package sarama

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestConsumerFetchFollowerFailover(t *testing.T) {
	const (
		topic  = "test.1"
		numMsg = 1000
	)

	newConfig := func() *Config {
		config := NewFunctionalTestConfig()
		config.ClientID = t.Name()
		config.Producer.Return.Successes = true
		return config
	}

	config := newConfig()

	// pick a partition and find the ID for one of the follower brokers
	admin, err := NewClusterAdmin(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	metadata, err := admin.DescribeTopics([]string{topic})
	if err != nil {
		t.Fatal(err)
	}
	partition := metadata[0].Partitions[0]
	leader := metadata[0].Partitions[0].Leader
	follower := int32(-1)
	for _, replica := range partition.Replicas {
		if replica == leader {
			continue
		}
		follower = replica
		break
	}

	t.Logf("topic %s has leader kafka-%d and our chosen follower is kafka-%d", topic, leader, follower)

	// match our clientID to the given broker so our requests should end up fetching from that follower
	config.RackID = strconv.FormatInt(int64(follower), 10)

	consumer, err := NewConsumer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}

	pc, err := consumer.ConsumePartition(topic, partition.ID, OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		pc.Close()
		consumer.Close()
	}()

	producer, err := NewSyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	var wg sync.WaitGroup
	wg.Add(numMsg)

	go func() {
		for i := 0; i < numMsg; i++ {
			msg := &ProducerMessage{
				Topic: topic, Key: nil, Value: StringEncoder(fmt.Sprintf("%s %-3d", t.Name(), i)),
			}
			if _, offset, err := producer.SendMessage(msg); err != nil {
				t.Error(i, err)
			} else if offset%50 == 0 {
				t.Logf("sent: %d\n", offset)
			}
			wg.Done()
			time.Sleep(time.Millisecond * 25)
		}
	}()

	i := 0

	for ; i < numMsg/8; i++ {
		msg := <-pc.Messages()
		if msg.Offset%50 == 0 {
			t.Logf("recv: %d\n", msg.Offset)
		}
	}

	if err := stopDockerTestBroker(context.Background(), follower); err != nil {
		t.Fatal(err)
	}

	for ; i < numMsg/3; i++ {
		msg := <-pc.Messages()
		if msg.Offset%50 == 0 {
			t.Logf("recv: %d\n", msg.Offset)
		}
	}

	if err := startDockerTestBroker(context.Background(), follower); err != nil {
		t.Fatal(err)
	}

	for ; i < numMsg; i++ {
		msg := <-pc.Messages()
		if msg.Offset%50 == 0 {
			t.Logf("recv: %d\n", msg.Offset)
		}
	}

	wg.Wait()
}

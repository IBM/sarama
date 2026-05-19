//go:build functional

package sarama

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)
	defer admin.Close()

	metadata, err := admin.DescribeTopics([]string{topic})
	require.NoError(t, err)
	partition := metadata[0].Partitions[0]
	leader := partition.Leader
	follower := int32(-1)
	for _, replica := range partition.Replicas {
		if replica == leader {
			continue
		}
		follower = replica
		break
	}
	require.NotEqual(t, int32(-1), follower, "no follower replica found")

	t.Logf("topic %s has leader kafka-%d and our chosen follower is kafka-%d", topic, leader, follower)

	// match our clientID to the given broker so our requests should end up fetching from that follower
	config.RackID = strconv.FormatInt(int64(follower), 10)

	consumer, err := NewConsumer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)

	pc, err := consumer.ConsumePartition(topic, partition.ID, OffsetOldest)
	require.NoError(t, err)
	defer func() {
		pc.Close()
		consumer.Close()
	}()

	producer, err := NewSyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer producer.Close()

	var lastProduced atomic.Int64
	lastProduced.Store(-1)

	var wg sync.WaitGroup
	wg.Add(numMsg)

	go func() {
		for i := 0; i < numMsg; i++ {
			msg := &ProducerMessage{
				Topic: topic, Key: nil, Value: StringEncoder(fmt.Sprintf("%s %-3d", t.Name(), i)),
			}
			if _, offset, err := producer.SendMessage(msg); err != nil {
				t.Error(i, err)
			} else {
				lastProduced.Store(offset)
				if offset%50 == 0 {
					t.Logf("sent: %d\n", offset)
				}
			}
			wg.Done()
			time.Sleep(time.Millisecond * 25)
		}
	}()

	const stallTimeout = 30 * time.Second
	recv := func(expected int) *ConsumerMessage {
		t.Helper()
		select {
		case msg := <-pc.Messages():
			return msg
		case err, ok := <-pc.Errors():
			if !ok {
				t.Fatalf("consumer error channel closed before offset %d", expected)
			}
			t.Fatalf("consumer error waiting for offset %d: %v", expected, err)
		case <-time.After(stallTimeout):
			t.Fatalf("consumer stalled waiting for offset %d after %s (HWM=%d, last produced=%d)",
				expected, stallTimeout, pc.HighWaterMarkOffset(), lastProduced.Load())
		}
		return nil
	}

	i := 0
	for ; i < numMsg/8; i++ {
		msg := recv(i)
		if msg.Offset%50 == 0 {
			t.Logf("recv: %d\n", msg.Offset)
		}
	}

	// register restart before stopping, so a Fatal below still restores the broker
	t.Cleanup(func() {
		if err := startDockerTestBroker(context.Background(), follower); err != nil {
			t.Fatal(err)
		}
	})
	require.NoError(t, stopDockerTestBroker(context.Background(), follower))

	// the leader pins HWM at the dead follower's last offset until it drops out of the ISR
	waitForISRShrink(t, admin, topic, partition.ID, follower, 60*time.Second)

	for ; i < numMsg/3; i++ {
		msg := recv(i)
		if msg.Offset%50 == 0 {
			t.Logf("recv: %d\n", msg.Offset)
		}
	}

	require.NoError(t, startDockerTestBroker(context.Background(), follower))

	for ; i < numMsg; i++ {
		msg := recv(i)
		if msg.Offset%50 == 0 {
			t.Logf("recv: %d\n", msg.Offset)
		}
	}

	wg.Wait()
}

// waitForISRShrink polls topic metadata until removed is no longer in the ISR for partition.
func waitForISRShrink(t *testing.T, admin ClusterAdmin, topic string, partition int32, removed int32, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastIsr []int32
	for {
		md, err := admin.DescribeTopics([]string{topic})
		if err != nil {
			t.Fatalf("describe topics while waiting for ISR shrink: %v", err)
		}
		for _, p := range md[0].Partitions {
			if p.ID == partition {
				lastIsr = p.Isr
				break
			}
		}
		if !slices.Contains(lastIsr, removed) {
			t.Logf("ISR for %s/%d shrank to %v (removed kafka-%d)", topic, partition, lastIsr, removed)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("kafka-%d still in ISR for %s/%d after %s (got %v)",
				removed, topic, partition, timeout, lastIsr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

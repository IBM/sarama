package sarama

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestDefaultConsumerConfigValidates(t *testing.T) {
	config := NewConsumerConfig()
	if err := config.Validate(); err != nil {
		t.Error(err)
	}
}

func TestDefaultPartitionConsumerConfigValidates(t *testing.T) {
	config := NewPartitionConsumerConfig()
	if err := config.Validate(); err != nil {
		t.Error(err)
	}
}

func TestConsumerOffsetManual(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, NoError)
	seedBroker.Returns(metadataResponse)

	for i := 0; i <= 10; i++ {
		fetchResponse := new(FetchResponse)
		fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(i+1234))
		leader.Returns(fetchResponse)
	}

	client, err := NewClient("client_id", []string{seedBroker.Addr()}, nil)

	if err != nil {
		t.Fatal(err)
	}

	master, err := NewConsumer(client, nil)
	if err != nil {
		t.Fatal(err)
	}

	config := NewPartitionConsumerConfig()
	config.OffsetMethod = OffsetMethodManual
	config.OffsetValue = 1234
	consumer, err := master.ConsumePartition("my_topic", 0, config)
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()

	for i := 0; i < 10; i++ {
		event := <-consumer.Events()
		if event.Err != nil {
			t.Error(event.Err)
		}
		if event.Offset != int64(i+1234) {
			t.Error("Incorrect message offset!")
		}
	}

	safeClose(t, consumer)
	safeClose(t, client)
	leader.Close()
}

func TestConsumerLatestOffset(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, NoError)
	seedBroker.Returns(metadataResponse)

	offsetResponse := new(OffsetResponse)
	offsetResponse.AddTopicPartition("my_topic", 0, 0x010101)
	leader.Returns(offsetResponse)

	fetchResponse := new(FetchResponse)
	fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), 0x010101)
	leader.Returns(fetchResponse)

	client, err := NewClient("client_id", []string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()

	master, err := NewConsumer(client, nil)
	if err != nil {
		t.Fatal(err)
	}

	config := NewPartitionConsumerConfig()
	config.OffsetMethod = OffsetMethodNewest
	consumer, err := master.ConsumePartition("my_topic", 0, config)
	if err != nil {
		t.Fatal(err)
	}

	leader.Close()
	safeClose(t, consumer)
	safeClose(t, client)

	// we deliver one message, so it should be one higher than we return in the OffsetResponse
	if consumer.offset != 0x010102 {
		t.Error("Latest offset not fetched correctly:", consumer.offset)
	}
}

func TestConsumerFunnyOffsets(t *testing.T) {
	// for topics that are compressed and/or compacted (different things!) we have to be
	// able to handle receiving offsets that are non-sequential (though still strictly increasing) and
	// possibly starting prior to the actual value we requested
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, NoError)
	seedBroker.Returns(metadataResponse)

	fetchResponse := new(FetchResponse)
	fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(1))
	fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(3))
	leader.Returns(fetchResponse)

	fetchResponse = new(FetchResponse)
	fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(5))
	leader.Returns(fetchResponse)

	client, err := NewClient("client_id", []string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	master, err := NewConsumer(client, nil)
	if err != nil {
		t.Fatal(err)
	}

	config := NewPartitionConsumerConfig()
	config.OffsetMethod = OffsetMethodManual
	config.OffsetValue = 2
	consumer, err := master.ConsumePartition("my_topic", 0, config)

	event := <-consumer.Events()
	if event.Err != nil {
		t.Error(event.Err)
	}
	if event.Offset != 3 {
		t.Error("Incorrect message offset!")
	}

	leader.Close()
	seedBroker.Close()
	safeClose(t, consumer)
	safeClose(t, client)
}

func TestConsumerRebalancingMultiplePartitions(t *testing.T) {
	// initial setup
	seedBroker := NewMockBroker(t, 1)
	leader0 := NewMockBroker(t, 2)
	leader1 := NewMockBroker(t, 3)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader0.Addr(), leader0.BrokerID())
	metadataResponse.AddBroker(leader1.Addr(), leader1.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader0.BrokerID(), nil, nil, NoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader1.BrokerID(), nil, nil, NoError)
	seedBroker.Returns(metadataResponse)

	// launch test goroutines
	client, err := NewClient("client_id", []string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	master, err := NewConsumer(client, nil)
	if err != nil {
		t.Fatal(err)
	}

	config := NewPartitionConsumerConfig()
	config.OffsetMethod = OffsetMethodManual
	config.OffsetValue = 0

	// we expect to end up (eventually) consuming exactly ten messages on each partition
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		consumer, err := master.ConsumePartition("my_topic", int32(i), config)
		if err != nil {
			t.Error(err)
		}
		wg.Add(1)
		go func(partition int32, c *PartitionConsumer) {
			for i := 0; i < 10; i++ {
				event := <-consumer.Events()
				if event.Err != nil {
					t.Error(event.Err, i, partition)
				}
				if event.Offset != int64(i) {
					t.Error("Incorrect message offset!", i, partition, event.Offset)
				}
				if event.Partition != partition {
					t.Error("Incorrect message partition!")
				}
			}
			safeClose(t, consumer)
			wg.Done()
		}(int32(i), consumer)
	}

	// leader0 provides first four messages on partition 0
	fetchResponse := new(FetchResponse)
	for i := 0; i < 4; i++ {
		fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(i))
	}
	leader0.Returns(fetchResponse)

	// leader0 says no longer leader of partition 0
	fetchResponse = new(FetchResponse)
	fetchResponse.AddError("my_topic", 0, NotLeaderForPartition)
	leader0.Returns(fetchResponse)

	// metadata assigns both partitions to leader1
	metadataResponse = new(MetadataResponse)
	metadataResponse.AddTopicPartition("my_topic", 0, leader1.BrokerID(), nil, nil, NoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader1.BrokerID(), nil, nil, NoError)
	seedBroker.Returns(metadataResponse)
	time.Sleep(5 * time.Millisecond) // dumbest way to force a particular response ordering

	// leader1 provides five messages on partition 1
	fetchResponse = new(FetchResponse)
	for i := 0; i < 5; i++ {
		fetchResponse.AddMessage("my_topic", 1, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(i))
	}
	leader1.Returns(fetchResponse)

	// leader1 provides three more messages on both partitions
	fetchResponse = new(FetchResponse)
	for i := 0; i < 3; i++ {
		fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(i+4))
		fetchResponse.AddMessage("my_topic", 1, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(i+5))
	}
	leader1.Returns(fetchResponse)

	// leader1 provides three more messages on partition0, says no longer leader of partition1
	fetchResponse = new(FetchResponse)
	for i := 0; i < 3; i++ {
		fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(i+7))
	}
	fetchResponse.AddError("my_topic", 1, NotLeaderForPartition)
	leader1.Returns(fetchResponse)

	// metadata assigns 0 to leader1 and 1 to leader0
	metadataResponse = new(MetadataResponse)
	metadataResponse.AddTopicPartition("my_topic", 0, leader1.BrokerID(), nil, nil, NoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader0.BrokerID(), nil, nil, NoError)
	seedBroker.Returns(metadataResponse)
	time.Sleep(5 * time.Millisecond) // dumbest way to force a particular response ordering

	// leader0 provides two messages on partition 1
	fetchResponse = new(FetchResponse)
	fetchResponse.AddMessage("my_topic", 1, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(8))
	fetchResponse.AddMessage("my_topic", 1, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(9))
	leader0.Returns(fetchResponse)

	// leader0 provides last message  on partition 1
	fetchResponse = new(FetchResponse)
	fetchResponse.AddMessage("my_topic", 1, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(10))
	leader0.Returns(fetchResponse)

	// leader1 provides last message  on partition 0
	fetchResponse = new(FetchResponse)
	fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(10))
	leader1.Returns(fetchResponse)

	wg.Wait()
	leader1.Close()
	leader0.Close()
	seedBroker.Close()
	safeClose(t, client)
}

func ExampleConsumer() {
	client, err := NewClient("my_client", []string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> connected")
	}
	defer client.Close()

	master, err := NewConsumer(client, nil)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> master consumer ready")
	}

	consumer, err := master.ConsumePartition("my_topic", 0, nil)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> consumer ready")
	}
	defer consumer.Close()

	msgCount := 0

consumerLoop:
	for {
		select {
		case event := <-consumer.Events():
			if event.Err != nil {
				panic(event.Err)
			}
			msgCount++
		case <-time.After(5 * time.Second):
			fmt.Println("> timed out")
			break consumerLoop
		}
	}
	fmt.Println("Got", msgCount, "messages.")
}

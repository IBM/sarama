package sarama

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestConsumerOffsetManual(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	leader := newMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	for i := 0; i <= 10; i++ {
		fetchResponse := new(FetchResponse)
		fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(i+1234))
		leader.Returns(fetchResponse)
	}

	master, err := NewConsumer([]string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	consumer, err := master.ConsumePartition("my_topic", 0, 1234)
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()

	for i := 0; i < 10; i++ {
		select {
		case message := <-consumer.Messages():
			if message.Offset != int64(i+1234) {
				t.Error("Incorrect message offset!")
			}
		case err := <-consumer.Errors():
			t.Error(err)
		}
	}

	safeClose(t, consumer)
	leader.Close()
}

func TestConsumerLatestOffset(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	leader := newMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	offsetResponse := new(OffsetResponse)
	offsetResponse.AddTopicPartition("my_topic", 0, 0x010101)
	leader.Returns(offsetResponse)

	fetchResponse := new(FetchResponse)
	fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), 0x010101)
	leader.Returns(fetchResponse)

	master, err := NewConsumer([]string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()

	consumer, err := master.ConsumePartition("my_topic", 0, OffsetNewest)
	if err != nil {
		t.Fatal(err)
	}

	leader.Close()
	safeClose(t, consumer)

	// we deliver one message, so it should be one higher than we return in the OffsetResponse
	if consumer.offset != 0x010102 {
		t.Error("Latest offset not fetched correctly:", consumer.offset)
	}
}

func TestConsumerFunnyOffsets(t *testing.T) {
	// for topics that are compressed and/or compacted (different things!) we have to be
	// able to handle receiving offsets that are non-sequential (though still strictly increasing) and
	// possibly starting prior to the actual value we requested
	seedBroker := newMockBroker(t, 1)
	leader := newMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	fetchResponse := new(FetchResponse)
	fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(1))
	fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(3))
	leader.Returns(fetchResponse)

	fetchResponse = new(FetchResponse)
	fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(5))
	leader.Returns(fetchResponse)

	master, err := NewConsumer([]string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	consumer, err := master.ConsumePartition("my_topic", 0, 2)

	message := <-consumer.Messages()
	if message.Offset != 3 {
		t.Error("Incorrect message offset!")
	}

	leader.Close()
	seedBroker.Close()
	safeClose(t, consumer)
}

func TestConsumerRebalancingMultiplePartitions(t *testing.T) {
	// initial setup
	seedBroker := newMockBroker(t, 1)
	leader0 := newMockBroker(t, 2)
	leader1 := newMockBroker(t, 3)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader0.Addr(), leader0.BrokerID())
	metadataResponse.AddBroker(leader1.Addr(), leader1.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader0.BrokerID(), nil, nil, ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader1.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	// launch test goroutines
	master, err := NewConsumer([]string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// we expect to end up (eventually) consuming exactly ten messages on each partition
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		consumer, err := master.ConsumePartition("my_topic", int32(i), 0)
		if err != nil {
			t.Error(err)
		}

		go func(c *PartitionConsumer) {
			for err := range c.Errors() {
				t.Error(err)
			}
		}(consumer)

		wg.Add(1)
		go func(partition int32, c *PartitionConsumer) {
			for i := 0; i < 10; i++ {
				message := <-consumer.Messages()
				if message.Offset != int64(i) {
					t.Error("Incorrect message offset!", i, partition, message.Offset)
				}
				if message.Partition != partition {
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
	fetchResponse.AddError("my_topic", 0, ErrNotLeaderForPartition)
	leader0.Returns(fetchResponse)

	// metadata assigns both partitions to leader1
	metadataResponse = new(MetadataResponse)
	metadataResponse.AddTopicPartition("my_topic", 0, leader1.BrokerID(), nil, nil, ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader1.BrokerID(), nil, nil, ErrNoError)
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
	fetchResponse.AddError("my_topic", 1, ErrNotLeaderForPartition)
	leader1.Returns(fetchResponse)

	// metadata assigns 0 to leader1 and 1 to leader0
	metadataResponse = new(MetadataResponse)
	metadataResponse.AddTopicPartition("my_topic", 0, leader1.BrokerID(), nil, nil, ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader0.BrokerID(), nil, nil, ErrNoError)
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
}

func ExampleConsumerWithSelect() {
	master, err := NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> master consumer ready")
	}
	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	consumer, err := master.ConsumePartition("my_topic", 0, 0)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> consumer ready")
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
	}()

	msgCount := 0

consumerLoop:
	for {
		select {
		case err := <-consumer.Errors():
			panic(err)
		case <-consumer.Messages():
			msgCount++
		case <-time.After(5 * time.Second):
			fmt.Println("> timed out")
			break consumerLoop
		}
	}
	fmt.Println("Got", msgCount, "messages.")
}

func ExampleConsumerWithGoroutines() {
	master, err := NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> master consumer ready")
	}
	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	consumer, err := master.ConsumePartition("my_topic", 0, 0)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> consumer ready")
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
	}()

	var (
		wg       sync.WaitGroup
		msgCount int
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for message := range consumer.Messages() {
			fmt.Printf("Consumed message with offset %d", message.Offset)
			msgCount++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range consumer.Errors() {
			fmt.Println(err)
		}
	}()

	wg.Wait()
	fmt.Println("Got", msgCount, "messages.")
}

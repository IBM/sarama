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
	mb1 := NewMockBroker(t, 1)
	mb2 := NewMockBroker(t, 2)

	mdr := new(MetadataResponse)
	mdr.AddBroker(mb2.Addr(), mb2.BrokerID())
	mdr.AddTopicPartition("my_topic", 0, 2, nil, nil, NoError)
	mb1.Returns(mdr)

	for i := 0; i <= 10; i++ {
		fr := new(FetchResponse)
		fr.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(i+1234))
		mb2.Returns(fr)
	}

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)

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
	mb1.Close()

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
	mb2.Close()
}

func TestConsumerLatestOffset(t *testing.T) {
	mb1 := NewMockBroker(t, 1)
	mb2 := NewMockBroker(t, 2)

	mdr := new(MetadataResponse)
	mdr.AddBroker(mb2.Addr(), mb2.BrokerID())
	mdr.AddTopicPartition("my_topic", 0, 2, nil, nil, NoError)
	mb1.Returns(mdr)

	or := new(OffsetResponse)
	or.AddTopicPartition("my_topic", 0, 0x010101)
	mb2.Returns(or)

	fr := new(FetchResponse)
	fr.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), 0x010101)
	mb2.Returns(fr)

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mb1.Close()

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

	mb2.Close()
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
	mb1 := NewMockBroker(t, 1)
	mb2 := NewMockBroker(t, 2)

	mdr := new(MetadataResponse)
	mdr.AddBroker(mb2.Addr(), mb2.BrokerID())
	mdr.AddTopicPartition("my_topic", 0, 2, nil, nil, NoError)
	mb1.Returns(mdr)

	fr := new(FetchResponse)
	fr.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(1))
	fr.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(3))
	mb2.Returns(fr)

	fr = new(FetchResponse)
	fr.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(5))
	mb2.Returns(fr)

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
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

	mb2.Close()
	mb1.Close()
	safeClose(t, consumer)
	safeClose(t, client)
}

func TestConsumerRebalancingMultiplePartitions(t *testing.T) {
	// initial setup
	mb1 := NewMockBroker(t, 1)
	mb2 := NewMockBroker(t, 2)
	mb3 := NewMockBroker(t, 3)

	mdr := new(MetadataResponse)
	mdr.AddBroker(mb2.Addr(), mb2.BrokerID())
	mdr.AddBroker(mb3.Addr(), mb3.BrokerID())
	mdr.AddTopicPartition("my_topic", 0, 2, nil, nil, NoError)
	mdr.AddTopicPartition("my_topic", 1, 3, nil, nil, NoError)
	mb1.Returns(mdr)

	// launch test goroutines
	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
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

	// generate broker responses
	fr := new(FetchResponse)
	for i := 0; i < 4; i++ {
		fr.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(i))
	}
	mb2.Returns(fr)

	fr = new(FetchResponse)
	fr.AddError("my_topic", 0, NotLeaderForPartition)
	mb2.Returns(fr)

	mdr = new(MetadataResponse)
	mdr.AddTopicPartition("my_topic", 0, 3, nil, nil, NoError)
	mdr.AddTopicPartition("my_topic", 1, 3, nil, nil, NoError)
	mb1.Returns(mdr)
	time.Sleep(5 * time.Millisecond) // dumbest way to force a particular response ordering

	fr = new(FetchResponse)
	for i := 0; i < 5; i++ {
		fr.AddMessage("my_topic", 1, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(i))
	}
	mb3.Returns(fr)

	fr = new(FetchResponse)
	for i := 0; i < 3; i++ {
		fr.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(i+4))
		fr.AddMessage("my_topic", 1, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(i+5))
	}
	mb3.Returns(fr)

	fr = new(FetchResponse)
	for i := 0; i < 3; i++ {
		fr.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(i+7))
	}
	fr.AddError("my_topic", 1, NotLeaderForPartition)
	mb3.Returns(fr)

	mdr = new(MetadataResponse)
	mdr.AddTopicPartition("my_topic", 0, 3, nil, nil, NoError)
	mdr.AddTopicPartition("my_topic", 1, 2, nil, nil, NoError)
	mb1.Returns(mdr)
	time.Sleep(5 * time.Millisecond) // dumbest way to force a particular response ordering

	fr = new(FetchResponse)
	fr.AddMessage("my_topic", 1, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(8))
	fr.AddMessage("my_topic", 1, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(9))
	mb2.Returns(fr)

	// cleanup
	fr = new(FetchResponse)
	fr.AddMessage("my_topic", 1, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(10))
	mb2.Returns(fr)

	fr = new(FetchResponse)
	fr.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(10))
	mb3.Returns(fr)

	wg.Wait()
	mb3.Close()
	mb2.Close()
	mb1.Close()
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

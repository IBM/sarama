package sarama

import (
	"log"
	"os"
	"os/signal"
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

	offsetResponseNewest := new(OffsetResponse)
	offsetResponseNewest.AddTopicPartition("my_topic", 0, 2345)
	leader.Returns(offsetResponseNewest)

	offsetResponseOldest := new(OffsetResponse)
	offsetResponseOldest.AddTopicPartition("my_topic", 0, 0)
	leader.Returns(offsetResponseOldest)

	for i := 0; i < 10; i++ {
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
	safeClose(t, master)
	leader.Close()
}

func TestConsumerOffsetNewest(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	leader := newMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	offsetResponseNewest := new(OffsetResponse)
	offsetResponseNewest.AddTopicPartition("my_topic", 0, 10)
	leader.Returns(offsetResponseNewest)

	offsetResponseOldest := new(OffsetResponse)
	offsetResponseOldest.AddTopicPartition("my_topic", 0, 7)
	leader.Returns(offsetResponseOldest)

	fetchResponse := new(FetchResponse)
	fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), 10)
	block := fetchResponse.GetBlock("my_topic", 0)
	block.HighWaterMarkOffset = 14
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

	msg := <-consumer.Messages()

	// we deliver one message, so it should be one higher than we return in the OffsetResponse
	if msg.Offset != 10 {
		t.Error("Latest message offset not fetched correctly:", msg.Offset)
	}

	if hwmo := consumer.HighWaterMarkOffset(); hwmo != 14 {
		t.Errorf("Expected high water mark offset 14, found %d", hwmo)
	}

	leader.Close()
	safeClose(t, consumer)
	safeClose(t, master)

	// We deliver one message, so it should be one higher than we return in the OffsetResponse.
	// This way it is set correctly for the next FetchRequest.
	if consumer.(*partitionConsumer).offset != 11 {
		t.Error("Latest offset not fetched correctly:", consumer.(*partitionConsumer).offset)
	}
}

func TestConsumerShutsDownOutOfRange(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	leader := newMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	offsetResponseNewest := new(OffsetResponse)
	offsetResponseNewest.AddTopicPartition("my_topic", 0, 1234)
	leader.Returns(offsetResponseNewest)

	offsetResponseOldest := new(OffsetResponse)
	offsetResponseOldest.AddTopicPartition("my_topic", 0, 0)
	leader.Returns(offsetResponseOldest)

	fetchResponse := new(FetchResponse)
	fetchResponse.AddError("my_topic", 0, ErrOffsetOutOfRange)
	leader.Returns(fetchResponse)

	master, err := NewConsumer([]string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()

	consumer, err := master.ConsumePartition("my_topic", 0, 101)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := <-consumer.Messages(); ok {
		t.Error("Expected the consumer to shut down")
	}

	leader.Close()
	safeClose(t, master)
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

	offsetResponseNewest := new(OffsetResponse)
	offsetResponseNewest.AddTopicPartition("my_topic", 0, 1234)
	leader.Returns(offsetResponseNewest)

	offsetResponseOldest := new(OffsetResponse)
	offsetResponseOldest.AddTopicPartition("my_topic", 0, 0)
	leader.Returns(offsetResponseOldest)

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
	if err != nil {
		t.Fatal(err)
	}

	if message := <-consumer.Messages(); message.Offset != 3 {
		t.Error("Incorrect message offset!")
	}

	if message := <-consumer.Messages(); message.Offset != 5 {
		t.Error("Incorrect message offset!")
	}

	leader.Close()
	seedBroker.Close()
	safeClose(t, consumer)
	safeClose(t, master)
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
	config := NewConfig()
	config.Consumer.Retry.Backoff = 0
	master, err := NewConsumer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	offsetResponseNewest0 := new(OffsetResponse)
	offsetResponseNewest0.AddTopicPartition("my_topic", 0, 1234)
	leader0.Returns(offsetResponseNewest0)

	offsetResponseOldest0 := new(OffsetResponse)
	offsetResponseOldest0.AddTopicPartition("my_topic", 0, 0)
	leader0.Returns(offsetResponseOldest0)

	offsetResponseNewest1 := new(OffsetResponse)
	offsetResponseNewest1.AddTopicPartition("my_topic", 1, 1234)
	leader1.Returns(offsetResponseNewest1)

	offsetResponseOldest1 := new(OffsetResponse)
	offsetResponseOldest1.AddTopicPartition("my_topic", 1, 0)
	leader1.Returns(offsetResponseOldest1)

	// we expect to end up (eventually) consuming exactly ten messages on each partition
	var wg sync.WaitGroup
	for i := int32(0); i < 2; i++ {
		consumer, err := master.ConsumePartition("my_topic", i, 0)
		if err != nil {
			t.Error(err)
		}

		go func(c PartitionConsumer) {
			for err := range c.Errors() {
				t.Error(err)
			}
		}(consumer)

		wg.Add(1)
		go func(partition int32, c PartitionConsumer) {
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
		}(i, consumer)
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
	time.Sleep(50 * time.Millisecond) // dumbest way to force a particular response ordering

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
	time.Sleep(50 * time.Millisecond) // dumbest way to force a particular response ordering

	// leader0 provides two messages on partition 1
	fetchResponse = new(FetchResponse)
	fetchResponse.AddMessage("my_topic", 1, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(8))
	fetchResponse.AddMessage("my_topic", 1, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(9))
	leader0.Returns(fetchResponse)
	time.Sleep(50 * time.Millisecond) // dumbest way to force a particular response ordering

	leader1.Close()
	leader0.Close()
	wg.Wait()
	seedBroker.Close()
	safeClose(t, master)
}

func TestConsumerInterleavedClose(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	leader := newMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	config := NewConfig()
	config.ChannelBufferSize = 0
	master, err := NewConsumer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	offsetResponseNewest0 := new(OffsetResponse)
	offsetResponseNewest0.AddTopicPartition("my_topic", 0, 1234)
	leader.Returns(offsetResponseNewest0)

	offsetResponseOldest0 := new(OffsetResponse)
	offsetResponseOldest0.AddTopicPartition("my_topic", 0, 0)
	leader.Returns(offsetResponseOldest0)

	c0, err := master.ConsumePartition("my_topic", 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	fetchResponse := new(FetchResponse)
	fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(0))
	leader.Returns(fetchResponse)
	time.Sleep(50 * time.Millisecond)

	offsetResponseNewest1 := new(OffsetResponse)
	offsetResponseNewest1.AddTopicPartition("my_topic", 1, 1234)
	leader.Returns(offsetResponseNewest1)

	offsetResponseOldest1 := new(OffsetResponse)
	offsetResponseOldest1.AddTopicPartition("my_topic", 1, 0)
	leader.Returns(offsetResponseOldest1)

	c1, err := master.ConsumePartition("my_topic", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	<-c0.Messages()

	fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(1))
	fetchResponse.AddMessage("my_topic", 1, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(0))
	leader.Returns(fetchResponse)

	safeClose(t, c1)
	safeClose(t, c0)
	safeClose(t, master)
	leader.Close()
	seedBroker.Close()
}

func TestConsumerBounceWithReferenceOpen(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	leader := newMockBroker(t, 2)
	leaderAddr := leader.Addr()
	tmp := newMockBroker(t, 3)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddBroker(tmp.Addr(), tmp.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 1, tmp.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	config := NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Retry.Backoff = 0
	config.ChannelBufferSize = 0
	master, err := NewConsumer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	offsetResponseNewest := new(OffsetResponse)
	offsetResponseNewest.AddTopicPartition("my_topic", 0, 1234)
	leader.Returns(offsetResponseNewest)

	offsetResponseOldest := new(OffsetResponse)
	offsetResponseOldest.AddTopicPartition("my_topic", 0, 0)
	leader.Returns(offsetResponseOldest)

	c0, err := master.ConsumePartition("my_topic", 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	offsetResponseNewest = new(OffsetResponse)
	offsetResponseNewest.AddTopicPartition("my_topic", 1, 1234)
	tmp.Returns(offsetResponseNewest)

	offsetResponseOldest = new(OffsetResponse)
	offsetResponseOldest.AddTopicPartition("my_topic", 1, 0)
	tmp.Returns(offsetResponseOldest)

	c1, err := master.ConsumePartition("my_topic", 1, 0)
	if err != nil {
		t.Fatal(err)
	}

	//redirect partition 1 back to main leader
	fetchResponse := new(FetchResponse)
	fetchResponse.AddError("my_topic", 1, ErrNotLeaderForPartition)
	tmp.Returns(fetchResponse)
	metadataResponse = new(MetadataResponse)
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)
	time.Sleep(5 * time.Millisecond)

	// now send one message to each partition to make sure everything is primed
	fetchResponse = new(FetchResponse)
	fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(0))
	fetchResponse.AddError("my_topic", 1, ErrNoError)
	leader.Returns(fetchResponse)
	<-c0.Messages()

	fetchResponse = new(FetchResponse)
	fetchResponse.AddError("my_topic", 0, ErrNoError)
	fetchResponse.AddMessage("my_topic", 1, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(0))
	leader.Returns(fetchResponse)
	<-c1.Messages()

	// bounce the broker
	leader.Close()
	leader = newMockBrokerAddr(t, 2, leaderAddr)

	// unblock one of the two (it doesn't matter which)
	select {
	case <-c0.Errors():
	case <-c1.Errors():
	}
	// send it back to the same broker
	seedBroker.Returns(metadataResponse)

	fetchResponse = new(FetchResponse)
	fetchResponse.AddMessage("my_topic", 0, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(1))
	fetchResponse.AddMessage("my_topic", 1, nil, ByteEncoder([]byte{0x00, 0x0E}), int64(1))
	leader.Returns(fetchResponse)

	time.Sleep(5 * time.Millisecond)

	// unblock the other one
	select {
	case <-c0.Errors():
	case <-c1.Errors():
	}
	// send it back to the same broker
	seedBroker.Returns(metadataResponse)

	time.Sleep(5 * time.Millisecond)

	select {
	case <-c0.Messages():
	case <-c1.Messages():
	}

	leader.Close()
	seedBroker.Close()
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		_ = c0.Close()
		wg.Done()
	}()
	go func() {
		_ = c1.Close()
		wg.Done()
	}()
	wg.Wait()
	safeClose(t, master)
	tmp.Close()
}

func TestConsumerOffsetOutOfRange(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	leader := newMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	master, err := NewConsumer([]string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()

	offsetResponseNewest := new(OffsetResponse)
	offsetResponseNewest.AddTopicPartition("my_topic", 0, 1234)

	offsetResponseOldest := new(OffsetResponse)
	offsetResponseOldest.AddTopicPartition("my_topic", 0, 2345)

	leader.Returns(offsetResponseNewest)
	leader.Returns(offsetResponseOldest)
	if _, err := master.ConsumePartition("my_topic", 0, 0); err != ErrOffsetOutOfRange {
		t.Fatal("Should return ErrOffsetOutOfRange, got:", err)
	}

	leader.Returns(offsetResponseNewest)
	leader.Returns(offsetResponseOldest)
	if _, err := master.ConsumePartition("my_topic", 0, 3456); err != ErrOffsetOutOfRange {
		t.Fatal("Should return ErrOffsetOutOfRange, got:", err)
	}

	leader.Returns(offsetResponseNewest)
	leader.Returns(offsetResponseOldest)
	if _, err := master.ConsumePartition("my_topic", 0, -3); err != ErrOffsetOutOfRange {
		t.Fatal("Should return ErrOffsetOutOfRange, got:", err)
	}

	leader.Close()
	safeClose(t, master)
}

// This example has the simplest use case of the consumer. It simply
// iterates over the messages channel using a for/range loop. Because
// a producer never stopsunless requested, a signal handler is registered
// so we can trigger a clean shutdown of the consumer.
func ExampleConsumer_for_loop() {
	master, err := NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := master.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	consumer, err := master.ConsumePartition("my_topic", 0, 0)
	if err != nil {
		log.Fatalln(err)
	}

	go func() {
		// By default, the consumer will always keep going, unless we tell it to stop.
		// In this case, we capture the SIGINT signal so we can tell the consumer to stop
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)
		<-signals
		consumer.AsyncClose()
	}()

	msgCount := 0
	for message := range consumer.Messages() {
		log.Println(string(message.Value))
		msgCount++
	}
	log.Println("Processed", msgCount, "messages.")
}

// This example shows how to use a consumer with a select statement
// dealing with the different channels.
func ExampleConsumer_select() {
	config := NewConfig()
	config.Consumer.Return.Errors = true // Handle errors manually instead of letting Sarama log them.

	master, err := NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := master.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	consumer, err := master.ConsumePartition("my_topic", 0, 0)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	msgCount := 0

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

consumerLoop:
	for {
		select {
		case err := <-consumer.Errors():
			log.Println(err)
		case <-consumer.Messages():
			msgCount++
		case <-signals:
			log.Println("Received interrupt")
			break consumerLoop
		}
	}
	log.Println("Processed", msgCount, "messages.")
}

// This example shows how to use a consumer with different goroutines
// to read from the Messages and Errors channels.
func ExampleConsumer_goroutines() {
	config := NewConfig()
	config.Consumer.Return.Errors = true // Handle errors manually instead of letting Sarama log them.

	master, err := NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	consumer, err := master.ConsumePartition("my_topic", 0, OffsetOldest)
	if err != nil {
		log.Fatalln(err)
	}

	var (
		wg       sync.WaitGroup
		msgCount int
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for message := range consumer.Messages() {
			log.Printf("Consumed message with offset %d", message.Offset)
			msgCount++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range consumer.Errors() {
			log.Println(err)
		}
	}()

	// Wait for an interrupt signal to trigger the shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
	consumer.AsyncClose()

	// Wait for the Messages and Errors channel to be fully drained.
	wg.Wait()
	log.Println("Processed", msgCount, "messages.")
}

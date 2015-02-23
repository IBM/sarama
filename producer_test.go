package sarama

import (
	"fmt"
	"sync"
	"testing"
)

const TestMessage = "ABC THE MESSAGE"

func closeProducer(t *testing.T, p *Producer) {
	var wg sync.WaitGroup
	p.AsyncClose()

	wg.Add(2)
	go func() {
		for _ = range p.Successes() {
			t.Error("Unexpected message on Successes()")
		}
		wg.Done()
	}()
	go func() {
		for msg := range p.Errors() {
			t.Error(msg.Err)
		}
		wg.Done()
	}()
	wg.Wait()
}

func TestDefaultProducerConfigValidates(t *testing.T) {
	config := NewProducerConfig()
	if err := config.Validate(); err != nil {
		t.Error(err)
	}
}

func TestSimpleProducer(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, 2, nil, nil, NoError)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, NoError)
	for i := 0; i < 10; i++ {
		leader.Returns(prodSuccess)
	}

	client, err := NewClient("client_id", []string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewSimpleProducer(client, nil)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		err = producer.SendMessage("my_topic", nil, StringEncoder(TestMessage))
		if err != nil {
			t.Error(err)
		}
	}

	safeClose(t, producer)
	safeClose(t, client)
	leader.Close()
	seedBroker.Close()
}

func TestConcurrentSimpleProducer(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, 2, nil, nil, NoError)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, NoError)
	leader.Returns(prodSuccess)

	client, err := NewClient("client_id", []string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	config := NewProducerConfig()
	config.FlushMsgCount = 100
	producer, err := NewSimpleProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			err := producer.SendMessage("my_topic", nil, StringEncoder(TestMessage))
			if err != nil {
				t.Error(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	safeClose(t, producer)
	safeClose(t, client)
	leader.Close()
	seedBroker.Close()
}

func TestProducer(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, NoError)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, NoError)
	leader.Returns(prodSuccess)

	client, err := NewClient("client_id", []string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	config := NewProducerConfig()
	config.FlushMsgCount = 10
	config.AckSuccesses = true
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage), Metadata: i}
	}
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
			if msg.Metadata.(int) != i {
				t.Error("Message metadata did not match")
			}
		}
	}

	closeProducer(t, producer)
	safeClose(t, client)
	leader.Close()
	seedBroker.Close()
}

func TestProducerMultipleFlushes(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, NoError)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, NoError)
	leader.Returns(prodSuccess)
	leader.Returns(prodSuccess)
	leader.Returns(prodSuccess)

	client, err := NewClient("client_id", []string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	config := NewProducerConfig()
	config.FlushMsgCount = 5
	config.AckSuccesses = true
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}

	for flush := 0; flush < 3; flush++ {
		for i := 0; i < 5; i++ {
			producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
		}
		for i := 0; i < 5; i++ {
			select {
			case msg := <-producer.Errors():
				t.Error(msg.Err)
				if msg.Msg.flags != 0 {
					t.Error("Message had flags set")
				}
			case msg := <-producer.Successes():
				if msg.flags != 0 {
					t.Error("Message had flags set")
				}
			}
		}
	}

	closeProducer(t, producer)
	safeClose(t, client)
	leader.Close()
	seedBroker.Close()
}

func TestProducerMultipleBrokers(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader0 := NewMockBroker(t, 2)
	leader1 := NewMockBroker(t, 3)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader0.Addr(), leader0.BrokerID())
	metadataResponse.AddBroker(leader1.Addr(), leader1.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader0.BrokerID(), nil, nil, NoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader1.BrokerID(), nil, nil, NoError)
	seedBroker.Returns(metadataResponse)

	prodResponse0 := new(ProduceResponse)
	prodResponse0.AddTopicPartition("my_topic", 0, NoError)
	leader0.Returns(prodResponse0)

	prodResponse1 := new(ProduceResponse)
	prodResponse1.AddTopicPartition("my_topic", 1, NoError)
	leader1.Returns(prodResponse1)

	client, err := NewClient("client_id", []string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	config := NewProducerConfig()
	config.FlushMsgCount = 5
	config.AckSuccesses = true
	config.Partitioner = NewRoundRobinPartitioner
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
		}
	}

	closeProducer(t, producer)
	safeClose(t, client)
	leader1.Close()
	leader0.Close()
	seedBroker.Close()
}

func TestProducerFailureRetry(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader1 := NewMockBroker(t, 2)
	leader2 := NewMockBroker(t, 3)

	metadataLeader1 := new(MetadataResponse)
	metadataLeader1.AddBroker(leader1.Addr(), leader1.BrokerID())
	metadataLeader1.AddTopicPartition("my_topic", 0, leader1.BrokerID(), nil, nil, NoError)
	seedBroker.Returns(metadataLeader1)

	client, err := NewClient("client_id", []string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	config := NewProducerConfig()
	config.FlushMsgCount = 10
	config.AckSuccesses = true
	config.RetryBackoff = 0
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()

	for i := 0; i < 10; i++ {
		producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	prodNotLeader := new(ProduceResponse)
	prodNotLeader.AddTopicPartition("my_topic", 0, NotLeaderForPartition)
	leader1.Returns(prodNotLeader)

	metadataLeader2 := new(MetadataResponse)
	metadataLeader2.AddBroker(leader2.Addr(), leader2.BrokerID())
	metadataLeader2.AddTopicPartition("my_topic", 0, leader2.BrokerID(), nil, nil, NoError)
	leader1.Returns(metadataLeader2)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, NoError)
	leader2.Returns(prodSuccess)
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
		}
	}
	leader1.Close()

	for i := 0; i < 10; i++ {
		producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	leader2.Returns(prodSuccess)
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
		}
	}

	leader2.Close()
	closeProducer(t, producer)
	safeClose(t, client)
}

func TestProducerBrokerBounce(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader := NewMockBroker(t, 2)
	leaderAddr := leader.Addr()

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leaderAddr, leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, NoError)
	seedBroker.Returns(metadataResponse)

	client, err := NewClient("client_id", []string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	config := NewProducerConfig()
	config.FlushMsgCount = 10
	config.AckSuccesses = true
	config.RetryBackoff = 0
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	leader.Close()                               // producer should get EOF
	leader = NewMockBrokerAddr(t, 2, leaderAddr) // start it up again right away for giggles
	seedBroker.Returns(metadataResponse)         // tell it to go to broker 2 again

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, NoError)
	leader.Returns(prodSuccess)
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
		}
	}
	seedBroker.Close()
	leader.Close()

	closeProducer(t, producer)
	safeClose(t, client)
}

func TestProducerBrokerBounceWithStaleMetadata(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader1 := NewMockBroker(t, 2)
	leader2 := NewMockBroker(t, 3)

	metadataLeader1 := new(MetadataResponse)
	metadataLeader1.AddBroker(leader1.Addr(), leader1.BrokerID())
	metadataLeader1.AddTopicPartition("my_topic", 0, leader1.BrokerID(), nil, nil, NoError)
	seedBroker.Returns(metadataLeader1)

	client, err := NewClient("client_id", []string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	config := NewProducerConfig()
	config.FlushMsgCount = 10
	config.AckSuccesses = true
	config.MaxRetries = 3
	config.RetryBackoff = 0
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	leader1.Close()                     // producer should get EOF
	seedBroker.Returns(metadataLeader1) // tell it to go to leader1 again even though it's still down
	seedBroker.Returns(metadataLeader1) // tell it to go to leader1 again even though it's still down

	// ok fine, tell it to go to leader2 finally
	metadataLeader2 := new(MetadataResponse)
	metadataLeader2.AddBroker(leader2.Addr(), leader2.BrokerID())
	metadataLeader2.AddTopicPartition("my_topic", 0, leader2.BrokerID(), nil, nil, NoError)
	seedBroker.Returns(metadataLeader2)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, NoError)
	leader2.Returns(prodSuccess)
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
		}
	}
	seedBroker.Close()
	leader2.Close()

	closeProducer(t, producer)
	safeClose(t, client)
}

func TestProducerMultipleRetries(t *testing.T) {
	seedBroker := NewMockBroker(t, 1)
	leader1 := NewMockBroker(t, 2)
	leader2 := NewMockBroker(t, 3)

	metadataLeader1 := new(MetadataResponse)
	metadataLeader1.AddBroker(leader1.Addr(), leader1.BrokerID())
	metadataLeader1.AddTopicPartition("my_topic", 0, leader1.BrokerID(), nil, nil, NoError)
	seedBroker.Returns(metadataLeader1)

	client, err := NewClient("client_id", []string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	config := NewProducerConfig()
	config.FlushMsgCount = 10
	config.AckSuccesses = true
	config.MaxRetries = 4
	config.RetryBackoff = 0
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	prodNotLeader := new(ProduceResponse)
	prodNotLeader.AddTopicPartition("my_topic", 0, NotLeaderForPartition)
	leader1.Returns(prodNotLeader)

	metadataLeader2 := new(MetadataResponse)
	metadataLeader2.AddBroker(leader2.Addr(), leader2.BrokerID())
	metadataLeader2.AddTopicPartition("my_topic", 0, leader2.BrokerID(), nil, nil, NoError)
	seedBroker.Returns(metadataLeader2)
	leader2.Returns(prodNotLeader)
	seedBroker.Returns(metadataLeader1)
	leader1.Returns(prodNotLeader)
	seedBroker.Returns(metadataLeader1)
	leader1.Returns(prodNotLeader)
	seedBroker.Returns(metadataLeader2)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, NoError)
	leader2.Returns(prodSuccess)
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
		}
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	leader2.Returns(prodSuccess)
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
		}
	}

	seedBroker.Close()
	leader1.Close()
	leader2.Close()
	closeProducer(t, producer)
	safeClose(t, client)
}

func ExampleProducer() {
	client, err := NewClient("client_id", []string{"localhost:9092"}, NewClientConfig())
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> connected")
	}
	defer client.Close()

	producer, err := NewProducer(client, nil)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	for {
		select {
		case producer.Input() <- &MessageToSend{Topic: "my_topic", Key: nil, Value: StringEncoder("testing 123")}:
			fmt.Println("> message queued")
		case err := <-producer.Errors():
			panic(err.Err)
		}
	}
}

func ExampleSimpleProducer() {
	client, err := NewClient("client_id", []string{"localhost:9092"}, NewClientConfig())
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> connected")
	}
	defer client.Close()

	producer, err := NewSimpleProducer(client, nil)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	for {
		err = producer.SendMessage("my_topic", nil, StringEncoder("testing 123"))
		if err != nil {
			panic(err)
		} else {
			fmt.Println("> message sent")
		}
	}
}

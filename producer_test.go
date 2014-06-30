package sarama

import (
	"fmt"
	"testing"
	"time"
)

const TestMessage = "ABC THE MESSAGE"

func defaultProducerConfig() *ProducerConfig {
	config := NewProducerConfig()
	config.MaxBufferTime = 1000000 * time.Millisecond             // don't flush based on time
	config.MaxBufferedBytes = uint32((len(TestMessage) * 10) - 1) // flush after 10 messages
	return config
}

func TestDefaultProducerConfigValidates(t *testing.T) {
	config := NewProducerConfig()
	if err := config.Validate(); err != nil {
		t.Error(err)
	}
}

func TestSimpleProducer(t *testing.T) {

	mb1 := NewMockBroker(t, 1)
	mb2 := NewMockBroker(t, 2)
	defer mb1.Close()
	defer mb2.Close()

	mdr := new(MetadataResponse)
	mdr.AddBroker(mb2.Addr(), mb2.BrokerID())
	mdr.AddTopicPartition("my_topic", 0, 2)
	mb1.Returns(mdr)

	pr := new(ProduceResponse)
	pr.AddTopicPartition("my_topic", 0, NoError)
	mb2.Returns(pr)

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewProducer(client, defaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	// flush only on 10th and final message
	returns := []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	for _, f := range returns {
		sendMessage(t, producer, "my_topic", TestMessage, f)
	}
}

func TestSimpleSyncProducer(t *testing.T) {

	mb1 := NewMockBroker(t, 1)
	mb2 := NewMockBroker(t, 2)
	defer mb1.Close()
	defer mb2.Close()

	mdr := new(MetadataResponse)
	mdr.AddBroker(mb2.Addr(), mb2.BrokerID())
	mdr.AddTopicPartition("my_topic", 1, 2)
	mb1.Returns(mdr)

	pr := new(ProduceResponse)
	pr.AddTopicPartition("my_topic", 1, NoError)

	for i := 0; i < 10; i++ {
		mb2.Returns(pr)
	}

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewProducer(client, defaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	for i := 0; i < 10; i++ {
		sendSyncMessage(t, producer, "my_topic", TestMessage)
	}
}

func TestMultipleFlushes(t *testing.T) {

	mb1 := NewMockBroker(t, 1)
	mb2 := NewMockBroker(t, 2)
	defer mb1.Close()
	defer mb2.Close()

	mdr := new(MetadataResponse)
	mdr.AddBroker(mb2.Addr(), mb2.BrokerID())
	mdr.AddTopicPartition("my_topic", 0, 2)
	mb1.Returns(mdr)

	pr := new(ProduceResponse)
	pr.AddTopicPartition("my_topic", 0, NoError)
	pr.AddTopicPartition("my_topic", 0, NoError)
	mb2.Returns(pr)
	mb2.Returns(pr) // yes, twice.

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	config := defaultProducerConfig()
	// So that we flush after the 2nd message.
	config.MaxBufferedBytes = uint32((len(TestMessage) * 5) - 1)
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	returns := []int{0, 0, 0, 0, 1, 0, 0, 0, 0, 1}
	for _, f := range returns {
		sendMessage(t, producer, "my_topic", TestMessage, f)
	}
}

func TestMultipleProducer(t *testing.T) {

	mb1 := NewMockBroker(t, 1)
	mb2 := NewMockBroker(t, 2)
	mb3 := NewMockBroker(t, 3)
	defer mb1.Close()
	defer mb2.Close()
	defer mb3.Close()

	mdr := new(MetadataResponse)
	mdr.AddBroker(mb2.Addr(), mb2.BrokerID())
	mdr.AddBroker(mb3.Addr(), mb3.BrokerID())
	mdr.AddTopicPartition("topic_a", 0, 2)
	mdr.AddTopicPartition("topic_b", 0, 3)
	mdr.AddTopicPartition("topic_c", 0, 3)
	mb1.Returns(mdr)

	pr1 := new(ProduceResponse)
	pr1.AddTopicPartition("topic_a", 0, NoError)
	mb2.Returns(pr1)

	pr2 := new(ProduceResponse)
	pr2.AddTopicPartition("topic_b", 0, NoError)
	pr2.AddTopicPartition("topic_c", 0, NoError)
	mb3.Returns(pr2)

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewProducer(client, defaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	// flush only on 10th and final message
	returns := []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	for _, f := range returns {
		sendMessage(t, producer, "topic_a", TestMessage, f)
	}

	// no flushes
	returns = []int{0, 0, 0, 0, 0}
	for _, f := range returns {
		sendMessage(t, producer, "topic_b", TestMessage, f)
	}

	// flush both topic_b and topic_c on 5th (ie. 10th for this broker)
	returns = []int{0, 0, 0, 0, 2}
	for _, f := range returns {
		sendMessage(t, producer, "topic_c", TestMessage, f)
	}
}

// Here we test that when two messages are sent in the same buffered request,
// and more messages are enqueued while the request is pending, everything
// happens correctly; that is, the first messages are retried before the next
// batch is allowed to submit.
func TestFailureRetry(t *testing.T) {
	t.Skip("not yet working after mockbroker refactor")

	mb1 := NewMockBroker(t, 1)
	mb2 := NewMockBroker(t, 2)
	mb3 := NewMockBroker(t, 3)

	mdr := new(MetadataResponse)
	mdr.AddBroker(mb2.Addr(), mb2.BrokerID())
	mdr.AddBroker(mb3.Addr(), mb3.BrokerID())
	mdr.AddTopicPartition("topic_a", 0, 2)
	mdr.AddTopicPartition("topic_b", 0, 3)
	mdr.AddTopicPartition("topic_c", 0, 3)
	mb1.Returns(mdr)

	/* mb1.ExpectMetadataRequest(). */
	/* 	AddBroker(mb2). */
	/* 	AddBroker(mb3). */
	/* 	AddTopicPartition("topic_a", 0, 2). */
	/* 	AddTopicPartition("topic_b", 0, 2). */
	/* 	AddTopicPartition("topic_c", 0, 3) */

	pr := new(ProduceResponse)
	pr.AddTopicPartition("topic_a", 0, NoError)
	pr.AddTopicPartition("topic_b", 0, NotLeaderForPartition)
	mb2.Returns(pr)

	/* mb2.ExpectProduceRequest(). */
	/* 	AddTopicPartition("topic_a", 0, 1, NoError). */
	/* 	AddTopicPartition("topic_b", 0, 1, NotLeaderForPartition) */

	// The fact that mb2 is chosen here is not well-defined. In theory,
	// it's a random choice between mb1, mb2, and mb3. Go's hash iteration
	// isn't quite as random as claimed, though, it seems. Maybe because
	// the same random seed is used each time?
	mdr2 := new(MetadataResponse)
	mdr2.AddBroker(mb3.Addr(), mb3.BrokerID())
	mdr2.AddTopicPartition("topic_b", 0, 3)
	mb2.Returns(mdr2)

	/* mb2.ExpectMetadataRequest(). */
	/* 	AddBroker(mb3). */
	/* 	AddTopicPartition("topic_b", 0, 3) */

	pr2 := new(ProduceResponse)
	pr2.AddTopicPartition("topic_c", 0, NoError)
	pr2.AddTopicPartition("topic_b", 0, NoError)
	mb3.Returns(pr2)

	/* mb3.ExpectProduceRequest(). */
	/* 	AddTopicPartition("topic_c", 0, 1, NoError). */
	/* 	AddTopicPartition("topic_b", 0, 1, NoError) */

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	producer, err := NewProducer(client, defaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	// Sent to mb3; does not flush because it's only half the cap.
	// mb1: [__]
	// mb2: [__]
	// mb3: [__]
	sendMessage(t, producer, "topic_c", TestMessage, 0)
	// mb1: [__]
	// mb2: [__]
	// mb3: [X_]

	// Sent to mb2; does not flush because it's only half the cap.
	sendMessage(t, producer, "topic_a", TestMessage, 0)
	// mb1: [__]
	// mb2: [X_]
	// mb3: [X_]

	// Sent to mb2; flushes, errors (retriable).
	// Three messages will be received:
	//   * NoError for topic_a;
	//   * NoError for topic_b;
	//   * NoError for topic_c.
	sendMessage(t, producer, "topic_b", TestMessage, 2)
	// mb1: [__]
	// mb2: [XX] <- flush!
	// mb3: [X_]

	// The topic_b message errors, and we should wind up here:

	// mb1: [__]
	// mb2: [__]
	// mb3: [XX] <- topic_b reassigned to mb3 by metadata refresh, flushes.

	defer mb1.Close()
	defer mb2.Close()
}

func readMessage(t *testing.T, ch chan error) {
	select {
	case err := <-ch:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(1 * time.Second):
		t.Error(fmt.Errorf("Message was never received"))
	}
}

func assertNoMessages(t *testing.T, ch chan error) {
	select {
	case x := <-ch:
		t.Fatal(fmt.Errorf("unexpected value received: %#v", x))
	case <-time.After(1 * time.Millisecond):
	}
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

	err = producer.SendMessage("my_topic", nil, StringEncoder("testing 123"))
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> message sent")
	}
}

func ExampleAsyncProducer() {
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

	go func() {
		for err := range producer.Errors() {
			panic(err)
		}
	}()

	for {
		err = producer.QueueMessage("my_topic", nil, StringEncoder("testing 123"))
		if err != nil {
			panic(err)
		} else {
			fmt.Println("> message sent")
		}
	}
}

func sendMessage(t *testing.T, producer *Producer, topic string, key string, expectedResponses int) {
	err := producer.QueueMessage(topic, nil, StringEncoder(key))
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < expectedResponses; i++ {
		readMessage(t, producer.Errors())
	}
	assertNoMessages(t, producer.Errors())
}

func sendSyncMessage(t *testing.T, producer *Producer, topic string, key string) {
	err := producer.SendMessage(topic, nil, StringEncoder(key))
	if err != nil {
		t.Error(err)
	}
	assertNoMessages(t, producer.Errors())
}

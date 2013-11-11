package sarama

import (
	"fmt"
	"github.com/shopify/sarama/mockbroker"
	"testing"
	"time"
)

const TestMessage = "ABC THE MESSAGE"

func TestSimpleProducer(t *testing.T) {

	mb1 := mockbroker.New(t, 1)
	mb2 := mockbroker.New(t, 2)
	defer mb1.Close()
	defer mb2.Close()

	mb1.ExpectMetadataRequest().
		AddBroker(mb2).
		AddTopicPartition("my_topic", 1, 2)

	mb2.ExpectProduceRequest().
		AddTopicPartition("my_topic", 1, 10, nil)

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewProducer(client, &ProducerConfig{
		RequiredAcks:  WaitForLocal,
		MaxBufferTime: 1000000, // "never"
		// So that we flush once, after the 10th message.
		MaxBufferBytes: uint32((len(TestMessage) * 10) - 1),
	})
	defer producer.Close()

	// flush only on 10th and final message
	returns := []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	for _, f := range returns {
		sendMessage(t, producer, "my_topic", TestMessage, f)
	}
}

func TestSimpleSyncProducer(t *testing.T) {

	mb1 := mockbroker.New(t, 1)
	mb2 := mockbroker.New(t, 2)
	defer mb1.Close()
	defer mb2.Close()

	mb1.ExpectMetadataRequest().
		AddBroker(mb2).
		AddTopicPartition("my_topic", 1, 2)

	for i := 0; i < 10; i++ {
		mb2.ExpectProduceRequest().
			AddTopicPartition("my_topic", 1, 10, nil)
	}

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewProducer(client, &ProducerConfig{
		RequiredAcks:  WaitForLocal,
		MaxBufferTime: 1000000, // "never"
		// So that we flush once, after the 10th message.
		MaxBufferBytes: uint32((len(TestMessage) * 10) - 1),
	})
	defer producer.Close()

	for i := 0; i < 10; i++ {
		sendSyncMessage(t, producer, "my_topic", TestMessage)
	}
}

func TestMultipleFlushes(t *testing.T) {
	t.Fatal("pending")

	mb1 := mockbroker.New(t, 1)
	mb2 := mockbroker.New(t, 2)
	defer mb1.Close()
	defer mb2.Close()

	mb1.ExpectMetadataRequest().
		AddBroker(mb2).
		AddTopicPartition("my_topic", 0, 2)

	mb2.ExpectProduceRequest().
		AddTopicPartition("my_topic", 0, 1, nil).
		AddTopicPartition("my_topic", 0, 1, nil)

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewProducer(client, &ProducerConfig{
		RequiredAcks:  WaitForLocal,
		MaxBufferTime: 1000000, // "never"
		// So that we flush once, after the 5th message.
		MaxBufferBytes: uint32((len(TestMessage) * 5) - 1),
	})
	defer producer.Close()

	returns := []int{0, 0, 0, 0, 1, 0, 0, 0, 0, 1}
	for _, f := range returns {
		sendMessage(t, producer, "my_topic", TestMessage, f)
	}
}

func TestMultipleProducer(t *testing.T) {
	t.Fatal("pending")

	mb1 := mockbroker.New(t, 1)
	mb2 := mockbroker.New(t, 2)
	mb3 := mockbroker.New(t, 3)
	defer mb1.Close()
	defer mb2.Close()
	defer mb3.Close()

	mb1.ExpectMetadataRequest().
		AddBroker(mb2).
		AddBroker(mb3).
		AddTopicPartition("topic_a", 0, 1).
		AddTopicPartition("topic_b", 0, 2).
		AddTopicPartition("topic_c", 0, 2)

	mb2.ExpectProduceRequest().
		AddTopicPartition("topic_a", 0, 1, nil)

	mb3.ExpectProduceRequest().
		AddTopicPartition("topic_b", 0, 1, nil).
		AddTopicPartition("topic_c", 0, 1, nil)

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewProducer(client, &ProducerConfig{
		RequiredAcks:  WaitForLocal,
		MaxBufferTime: 1000000, // "never"
		// So that we flush once, after the 10th message.
		MaxBufferBytes: uint32((len(TestMessage) * 10) - 1),
	})
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
	t.Fatal("pending")

	mb1 := mockbroker.New(t, 1)
	mb2 := mockbroker.New(t, 2)
	defer mb1.Close()
	defer mb2.Close()

	mb1.ExpectMetadataRequest().
		AddBroker(mb2).
		AddTopicPartition("topic_a", 0, 1).
		AddTopicPartition("topic_b", 0, 2).
		AddTopicPartition("topic_c", 0, 2)

	mb2.ExpectProduceRequest().
		AddTopicPartition("topic_b", 0, 1, NotLeaderForPartition).
		AddTopicPartition("topic_c", 0, 1, nil)

	mb1.ExpectMetadataRequest().
		AddBroker(mb2).
		AddTopicPartition("topic_b", 0, 1)

	mb1.ExpectProduceRequest().
		AddTopicPartition("topic_a", 0, 1, nil).
		AddTopicPartition("topic_b", 0, 1, nil)

	client, err := NewClient("client_id", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	producer, err := NewProducer(client, &ProducerConfig{
		RequiredAcks:  WaitForLocal,
		MaxBufferTime: 1000000, // "never"
		// So that we flush after the 2nd message.
		MaxBufferBytes:     uint32((len(TestMessage) * 2) - 1),
		MaxDeliveryRetries: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	// Sent to the first BP; does not flush because it's only half the cap.
	println("WTF1")
	sendMessage(t, producer, "topic_a", TestMessage, 0)
	// Sent to the first BP; flushes, errors (retriable).
	// There's a delay, during which the next message is enqueued to the first BP,
	// after which the BP is closed and the message is re-enqueued to the second
	// BP. This BP is not flushed immediately because it is only at half-cap.
	println("WTF2")
	sendMessage(t, producer, "topic_b", TestMessage, 1)
	// This happens before the BP is terminated, and the message is enqueued to
	// the first BP. It is not immediately flushed, because it is at half-cap.
	println("WTF")
	sendMessage(t, producer, "topic_b", TestMessage, 1)

	// Now the Close() runs on the first BP. The BP has buffered the second
	// message (which previously failed). This forces a flush.

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
	client, err := NewClient("my_client", []string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> connected")
	}
	defer client.Close()

	producer, err := NewProducer(client, &ProducerConfig{RequiredAcks: WaitForLocal})
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

package sarama

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

func TestSimpleProducer(t *testing.T) {
	responses := make(chan []byte, 1)
	extraResponses := make(chan []byte)
	mockBroker := NewMockBroker(t, responses)
	mockExtra := NewMockBroker(t, extraResponses)
	defer mockBroker.Close()
	defer mockExtra.Close()

	// return the extra mock as another available broker
	response := []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}
	binary.BigEndian.PutUint32(response[19:], uint32(mockExtra.Port()))
	responses <- response
	go func() {
		msg := []byte{
			0x00, 0x00, 0x00, 0x01,
			0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c',
			0x00, 0x00, 0x00, 0x01,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
		binary.BigEndian.PutUint64(msg[23:], 0)
		extraResponses <- msg
	}()

	client, err := NewClient("client_id", []string{mockBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewProducer(client, &ProducerConfig{
		RequiredAcks:  WaitForLocal,
		MaxBufferTime: 1000000, // "never"
		// So that we flush once, after the 10th message.
		MaxBufferBytes: uint32((len("ABC THE MESSAGE") * 10) - 1),
	})
	defer producer.Close()

	// flush only on 10th and final message
	returns := []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	for _, f := range returns {
		sendMessage(t, producer, "my_topic", "ABC THE MESSAGE", f)
	}
}

func TestSimpleSyncProducer(t *testing.T) {
	responses := make(chan []byte, 1)
	extraResponses := make(chan []byte)
	mockBroker := NewMockBroker(t, responses)
	mockExtra := NewMockBroker(t, extraResponses)
	defer mockBroker.Close()
	defer mockExtra.Close()

	// return the extra mock as another available broker
	response := []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}
	binary.BigEndian.PutUint32(response[19:], uint32(mockExtra.Port()))
	responses <- response
	go func() {
		msg := []byte{
			0x00, 0x00, 0x00, 0x01,
			0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c',
			0x00, 0x00, 0x00, 0x01,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
		binary.BigEndian.PutUint64(msg[23:], 0)
		for i := 0; i < 10; i++ {
			extraResponses <- msg
		}
	}()

	client, err := NewClient("client_id", []string{mockBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewProducer(client, &ProducerConfig{
		RequiredAcks:  WaitForLocal,
		MaxBufferTime: 1000000, // "never"
		// So that we flush once, after the 10th message.
		MaxBufferBytes: uint32((len("ABC THE MESSAGE") * 10) - 1),
	})
	defer producer.Close()

	for i := 0; i < 10; i++ {
		sendSyncMessage(t, producer, "my_topic", "ABC THE MESSAGE")
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

func TestMultipleFlushes(t *testing.T) {
	responses := make(chan []byte, 1)
	extraResponses := make(chan []byte)
	mockBroker := NewMockBroker(t, responses)
	mockExtra := NewMockBroker(t, extraResponses)
	defer mockBroker.Close()
	defer mockExtra.Close()

	// return the extra mock as another available broker
	response := []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}
	binary.BigEndian.PutUint32(response[19:], uint32(mockExtra.Port()))
	responses <- response
	go func() {
		msg := []byte{
			0x00, 0x00, 0x00, 0x01,
			0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c',
			0x00, 0x00, 0x00, 0x01,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00,
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		}
		binary.BigEndian.PutUint64(msg[23:], 0)
		extraResponses <- msg
		extraResponses <- msg
	}()

	client, err := NewClient("client_id", []string{mockBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewProducer(client, &ProducerConfig{
		RequiredAcks:  WaitForLocal,
		MaxBufferTime: 1000000, // "never"
		// So that we flush once, after the 5th message.
		MaxBufferBytes: uint32((len("ABC THE MESSAGE") * 5) - 1),
	})
	defer producer.Close()

	returns := []int{0, 0, 0, 0, 1, 0, 0, 0, 0, 1}
	for _, f := range returns {
		sendMessage(t, producer, "my_topic", "ABC THE MESSAGE", f)
	}
}

func TestMultipleProducer(t *testing.T) {
	responses := make(chan []byte, 1)
	responsesA := make(chan []byte)
	responsesB := make(chan []byte)
	mockBroker := NewMockBroker(t, responses)
	mockBrokerA := NewMockBroker(t, responsesA)
	mockBrokerB := NewMockBroker(t, responsesB)
	defer mockBroker.Close()
	defer mockBrokerA.Close()
	defer mockBrokerB.Close()

	// We're going to return:
	// topic: topic_a; partition: 0; brokerID: 1
	// topic: topic_b; partition: 0; brokerID: 2
	// topic: topic_c; partition: 0; brokerID: 2

	// Return the extra broker metadata so that the producer will send
	// requests to mockBrokerA and mockBrokerB.
	response := []byte{
		0x00, 0x00, 0x00, 0x02, // 0:3 number of brokers

		0x00, 0x00, 0x00, 0x01, // 4:7 broker ID
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't', // 8:18 hostname
		0xFF, 0xFF, 0xFF, 0xFF, // 19:22 port will be written here.

		0x00, 0x00, 0x00, 0x02, // 23:26 broker ID
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't', // 27:37 hostname
		0xFF, 0xFF, 0xFF, 0xFF, // 38:41 port will be written here.

		0x00, 0x00, 0x00, 0x03, // number of topic metadata records

		0x00, 0x00, // error: 0 means no error
		0x00, 0x07, 't', 'o', 'p', 'i', 'c', '_', 'a', // topic name
		0x00, 0x00, 0x00, 0x01, // number of partition metadata records for this topic
		0x00, 0x00, // error: 0 means no error
		0x00, 0x00, 0x00, 0x00, // partition ID
		0x00, 0x00, 0x00, 0x01, // broker ID of leader
		0x00, 0x00, 0x00, 0x00, // replica set
		0x00, 0x00, 0x00, 0x00, // ISR set

		0x00, 0x00, // error: 0 means no error
		0x00, 0x07, 't', 'o', 'p', 'i', 'c', '_', 'b', // topic name
		0x00, 0x00, 0x00, 0x01, // number of partition metadata records for this topic
		0x00, 0x00, // error: 0 means no error
		0x00, 0x00, 0x00, 0x00, // partition ID
		0x00, 0x00, 0x00, 0x02, // broker ID of leader
		0x00, 0x00, 0x00, 0x00, // replica set
		0x00, 0x00, 0x00, 0x00, // ISR set

		0x00, 0x00, // error: 0 means no error
		0x00, 0x07, 't', 'o', 'p', 'i', 'c', '_', 'c', // topic name
		0x00, 0x00, 0x00, 0x01, // number of partition metadata records for this topic
		0x00, 0x00, // error: 0 means no error
		0x00, 0x00, 0x00, 0x00, // partition ID
		0x00, 0x00, 0x00, 0x02, // broker ID of leader
		0x00, 0x00, 0x00, 0x00, // replica set
		0x00, 0x00, 0x00, 0x00, // ISR set

	}
	binary.BigEndian.PutUint32(response[19:], uint32(mockBrokerA.Port()))
	binary.BigEndian.PutUint32(response[38:], uint32(mockBrokerB.Port()))
	responses <- response

	go func() {
		msg := []byte{
			0x00, 0x00, 0x00, 0x01, // 0:3 number of topics
			0x00, 0x07, 't', 'o', 'p', 'i', 'c', '_', 'a', // 4:12 topic name
			0x00, 0x00, 0x00, 0x01, // 13:16 number of blocks for this topic
			0x00, 0x00, 0x00, 0x00, // 17:20 partition id
			0x00, 0x00, // 21:22 error: 0 means no error
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 23:30 offset
		}
		binary.BigEndian.PutUint64(msg[23:], 0)
		responsesA <- msg
	}()

	go func() {
		msg := []byte{
			0x00, 0x00, 0x00, 0x02, // 0:3 number of topics

			0x00, 0x07, 't', 'o', 'p', 'i', 'c', '_', 'b', // 4:12 topic name
			0x00, 0x00, 0x00, 0x01, // 13:16 number of blocks for this topic
			0x00, 0x00, 0x00, 0x00, // 17:20 partition id
			0x00, 0x00, // 21:22 error: 0 means no error
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 23:30 offset

			0x00, 0x07, 't', 'o', 'p', 'i', 'c', '_', 'c', // 4:12 topic name
			0x00, 0x00, 0x00, 0x01, // 13:16 number of blocks for this topic
			0x00, 0x00, 0x00, 0x00, // 17:20 partition id
			0x00, 0x00, // 21:22 error: 0 means no error
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 23:30 offset
		}
		binary.BigEndian.PutUint64(msg[23:], 0)
		responsesB <- msg
	}()

	client, err := NewClient("client_id", []string{mockBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewProducer(client, &ProducerConfig{
		RequiredAcks:  WaitForLocal,
		MaxBufferTime: 1000000, // "never"
		// So that we flush once, after the 10th message.
		MaxBufferBytes: uint32((len("ABC THE MESSAGE") * 10) - 1),
	})
	defer producer.Close()

	// flush only on 10th and final message
	returns := []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	for _, f := range returns {
		sendMessage(t, producer, "topic_a", "ABC THE MESSAGE", f)
	}

	// no flushes
	returns = []int{0, 0, 0, 0, 0}
	for _, f := range returns {
		sendMessage(t, producer, "topic_b", "ABC THE MESSAGE", f)
	}

	// flush both topic_b and topic_c on 5th (ie. 10th for this broker)
	returns = []int{0, 0, 0, 0, 2}
	for _, f := range returns {
		sendMessage(t, producer, "topic_c", "ABC THE MESSAGE", f)
	}
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
		t.Error(fmt.Errorf("unexpected value received: %#v", x))
	case <-time.After(5 * time.Millisecond):
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

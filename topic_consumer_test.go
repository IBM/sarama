package sarama

import (
	"testing"
	"time"
)

func collectMessages(t *testing.T, c <-chan *ConsumerMessage, count int) []string {
	messages := []string{}

	for i := 0; i < count; i++ {
		timeout := make(chan bool, 1)

		go func() {
			time.Sleep(1000 * time.Millisecond)
			timeout <- true
		}()

		select {
		case m := <-c:
			msg := string(m.Value[:])
			messages = append(messages, msg)
		case <-timeout:
			t.Errorf("Expected %v more messages in channel but there was none", i)
			t.FailNow()
		}
	}

	return messages
}

func safeProduceStringMsg(t *testing.T, p SyncProducer, topic, msg string, partition int32) {
	_, _, err := p.SendMessage(&ProducerMessage{Topic: topic, Value: StringEncoder(msg), Partition: partition})
	failOnErr(t, err)
}

func failOnErr(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestTopicConsumer(t *testing.T) {
	server, err := NewKafkaTestServer(t)
	failOnErr(t, err)
	server.Up()

	conf := NewConfig()
	conf.Version = V0_10_0_0
	conf.Producer.Partitioner = NewManualPartitioner
	conf.Producer.Return.Successes = true
	// conf.Consumer.Offsets.Initial = OffsetOldest
	client, err := NewClient([]string{server.Address()}, conf)
	failOnErr(t, err)

	p, err := NewSyncProducerFromClient(client)
	failOnErr(t, err)

	safeProduceStringMsg(t, p, "test", "msg1", 1)
	safeProduceStringMsg(t, p, "test", "msg2", 0)
	safeProduceStringMsg(t, p, "test", "msg3", 1)
	safeProduceStringMsg(t, p, "test", "msg4", 0)
	p.Close()

	consumer, err := NewTopicConsumer(client, "test", make(map[int32]int64), false)
	failOnErr(t, err)

	messages := collectMessages(t, consumer.Messages(), 4)

	expectedMessages := []string{"msg1", "msg2", "msg3", "msg4"}

	for _, expectedMsg := range expectedMessages {
		contained := false
		for _, msg := range messages {
			if !contained && msg == expectedMsg {
				contained = true
			}
		}

		if !contained {
			t.Errorf("Expected message %v, but it was not in %v", expectedMsg, messages)
		}
	}

	failOnErr(t, consumer.Close())
	server.Down()
}

func TestTopicConsumerSettingOffsets(t *testing.T) {
	server, err := NewKafkaTestServer(t)
	failOnErr(t, err)

	server.Up()

	conf := NewConfig()
	conf.Version = V0_10_0_0
	conf.Producer.Partitioner = NewManualPartitioner
	conf.Producer.Return.Successes = true
	client, err := NewClient([]string{server.Address()}, conf)
	failOnErr(t, err)

	p, err := NewSyncProducerFromClient(client)
	failOnErr(t, err)

	safeProduceStringMsg(t, p, "test", "msg1", 1)
	safeProduceStringMsg(t, p, "test", "msg2", 0)
	safeProduceStringMsg(t, p, "test", "msg3", 1)
	safeProduceStringMsg(t, p, "test", "msg4", 1)
	failOnErr(t, p.Close())

	offsets := map[int32]int64{
		0: 1,
		1: 1,
	}

	consumer, err := NewTopicConsumer(client, "test", offsets, false)
	failOnErr(t, err)

	messages := collectMessages(t, consumer.Messages(), 2)

	expectedMessages := []string{"msg3", "msg4"}

	for _, expectedMsg := range expectedMessages {
		contained := false
		for _, msg := range messages {
			if !contained && msg == expectedMsg {
				contained = true
			}
		}

		if !contained {
			t.Errorf("Expected messages %v, but it was not in %v", expectedMsg, messages)
		}
	}

	failOnErr(t, consumer.Close())
	server.Down()
}

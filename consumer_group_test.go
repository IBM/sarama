package sarama

import (
	"context"
	"fmt"
	"testing"
)

func TestConsumerGroupSessionNextOffset(t *testing.T) {
	var partition int32 = 13
	var offset int64 = 23
	var meta = "meta"
	poms := map[string]map[int32]*partitionOffsetManager{
		"topic1": {
			partition: {
				offset:   offset,
				metadata: meta,
			},
		},
	}

	sess := consumerGroupSession{offsets: &offsetManager{poms: poms}}

	actualOffset, actualMetadata, err := sess.NextOffset("topic1", partition)
	if err != nil {
		t.Errorf("partition should be claimed by this session. err: %s", err.Error())
	}

	if actualOffset != offset {
		t.Errorf("expected next offset: %d, got: %d", offset, actualOffset)
	}

	if actualMetadata != meta {
		t.Errorf("expected meta: %s, got %s", meta, actualMetadata)
	}

	// check we don't get offset for an unclaimed partition
	_, _, err = sess.NextOffset("topic1", 5)
	if err != ErrPartitionNotClaimed {
		t.Error("partition should not be claimed by this session")
	}

	// check we don't get offset for an unclaimed topic
	_, _, err = sess.NextOffset("topic2", 23)
	if err != ErrPartitionNotClaimed {
		t.Error("topic should not be claimed by this session")
	}
}

type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(_ ConsumerGroupSession) error   { return nil }
func (exampleConsumerGroupHandler) Cleanup(_ ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(sess ConsumerGroupSession, claim ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		sess.MarkMessage(msg, "")
	}
	return nil
}

func ExampleConsumerGroup() {
	// Init config, specify appropriate version
	config := NewConfig()
	config.Version = V1_0_0_0
	config.Consumer.Return.Errors = true

	// Start with a client
	client, err := NewClient([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()

	// Start a new consumer group
	group, err := NewConsumerGroupFromClient("my-group", client)
	if err != nil {
		panic(err)
	}
	defer func() { _ = group.Close() }()

	// Track errors
	go func() {
		for err := range group.Errors() {
			fmt.Println("ERROR", err)
		}
	}()

	// Iterate over consumer sessions.
	ctx := context.Background()
	for {
		topics := []string{"my-topic"}
		handler := exampleConsumerGroupHandler{}

		err := group.Consume(ctx, topics, handler)
		if err != nil {
			panic(err)
		}
	}
}

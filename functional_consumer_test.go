package sarama

import (
	"math"
	"testing"
)

func TestFuncConsumerOffsetOutOfRange(t *testing.T) {
	checkKafkaAvailability(t)

	consumer, err := NewConsumer(kafkaBrokers, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := consumer.ConsumePartition("test.1", 0, -10); err != ErrOffsetOutOfRange {
		t.Error("Expected ErrOffsetOutOfRange, got:", err)
	}

	if _, err := consumer.ConsumePartition("test.1", 0, math.MaxInt64); err != ErrOffsetOutOfRange {
		t.Error("Expected ErrOffsetOutOfRange, got:", err)
	}

	safeClose(t, consumer)
}

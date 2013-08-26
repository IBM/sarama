package sarama

import (
	"crypto/rand"
	"testing"
)

func assertPartitioningConsistent(t *testing.T, partitioner Partitioner, key Encoder, numPartitions int32) {
	choice := partitioner.Partition(key, numPartitions)
	if choice < 0 || choice >= numPartitions {
		t.Error(partitioner, "returned partition", choice, "outside of range for", key)
	}
	for i := 1; i < 50; i++ {
		newChoice := partitioner.Partition(key, numPartitions)
		if newChoice != choice {
			t.Error(partitioner, "returned partition", newChoice, "inconsistent with", choice, ".")
		}
	}
}

func TestRandomPartitioner(t *testing.T) {
	partitioner := NewRandomPartitioner()

	choice := partitioner.Partition(nil, 1)
	if choice != 0 {
		t.Error("Returned non-zero partition when only one available.")
	}

	for i := 1; i < 50; i++ {
		choice := partitioner.Partition(nil, 50)
		if choice < 0 || choice >= 50 {
			t.Error("Returned partition", choice, "outside of range.")
		}
	}
}

func TestRoundRobinPartitioner(t *testing.T) {
	partitioner := RoundRobinPartitioner{}

	choice := partitioner.Partition(nil, 1)
	if choice != 0 {
		t.Error("Returned non-zero partition when only one available.")
	}

	var i int32
	for i = 1; i < 50; i++ {
		choice := partitioner.Partition(nil, 7)
		if choice != i%7 {
			t.Error("Returned partition", choice, "expecting", i%7)
		}
	}
}

func TestHashPartitioner(t *testing.T) {
	partitioner := NewHashPartitioner()

	choice := partitioner.Partition(nil, 1)
	if choice != 0 {
		t.Error("Returned non-zero partition when only one available.")
	}

	for i := 1; i < 50; i++ {
		choice := partitioner.Partition(nil, 50)
		if choice < 0 || choice >= 50 {
			t.Error("Returned partition", choice, "outside of range for nil key.")
		}
	}

	buf := make([]byte, 256)
	for i := 1; i < 50; i++ {
		rand.Read(buf)
		assertPartitioningConsistent(t, partitioner, ByteEncoder(buf), 50)
	}
}

package sarama

import (
	"crypto/rand"
	"testing"
)

func assertPartitioningConsistent(t *testing.T, partitioner Partitioner, key Encoder, numPartitions int32) {
	choice, err := partitioner.Partition(key, numPartitions)
	if err != nil {
		t.Error(partitioner, err)
	}
	if choice < 0 || choice >= numPartitions {
		t.Error(partitioner, "returned partition", choice, "outside of range for", key)
	}
	for i := 1; i < 50; i++ {
		newChoice, err := partitioner.Partition(key, numPartitions)
		if err != nil {
			t.Error(partitioner, err)
		}
		if newChoice != choice {
			t.Error(partitioner, "returned partition", newChoice, "inconsistent with", choice, ".")
		}
	}
}

func TestRandomPartitioner(t *testing.T) {
	partitioner := NewRandomPartitioner()

	choice, err := partitioner.Partition(nil, 1)
	if err != nil {
		t.Error(partitioner, err)
	}
	if choice != 0 {
		t.Error("Returned non-zero partition when only one available.")
	}

	for i := 1; i < 50; i++ {
		choice, err := partitioner.Partition(nil, 50)
		if err != nil {
			t.Error(partitioner, err)
		}
		if choice < 0 || choice >= 50 {
			t.Error("Returned partition", choice, "outside of range.")
		}
	}
}

func TestRoundRobinPartitioner(t *testing.T) {
	partitioner := RoundRobinPartitioner{}

	choice, err := partitioner.Partition(nil, 1)
	if err != nil {
		t.Error(partitioner, err)
	}
	if choice != 0 {
		t.Error("Returned non-zero partition when only one available.")
	}

	var i int32
	for i = 1; i < 50; i++ {
		choice, err := partitioner.Partition(nil, 7)
		if err != nil {
			t.Error(partitioner, err)
		}
		if choice != i%7 {
			t.Error("Returned partition", choice, "expecting", i%7)
		}
	}
}

func TestHashPartitioner(t *testing.T) {
	partitioner := NewHashPartitioner()

	choice, err := partitioner.Partition(nil, 1)
	if err != nil {
		t.Error(partitioner, err)
	}
	if choice != 0 {
		t.Error("Returned non-zero partition when only one available.")
	}

	for i := 1; i < 50; i++ {
		choice, err := partitioner.Partition(nil, 50)
		if err != nil {
			t.Error(partitioner, err)
		}
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

func TestConstantPartitioner(t *testing.T) {
	var partitioner Partitioner
	partitioner = &ConstantPartitioner{Constant: 0}

	for i := 1; i < 50; i++ {
		choice, err := partitioner.Partition(nil, 50)
		if err != nil {
			t.Error(partitioner, err)
		}
		if choice != 0 {
			t.Error("Returned partition", choice, "instead of 0.")
		}
	}
}

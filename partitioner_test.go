package kafka

import "testing"

func TestRandomPartitioner(t *testing.T) {
	partitioner := RandomPartitioner{}

	choice := partitioner.Partition(nil, 1)
	if choice != 0 {
		t.Error("Returned non-zero partition when only one available.")
	}

	for i := 1; i < 50; i++ {
		choice := partitioner.Partition(nil, 50)
		if choice < 0 || choice >= 50 {
			t.Fatal("Returned partition", choice, "outside of range.")
		}
	}
}

func TestRoundRobinPartitioner(t *testing.T) {
	partitioner := RoundRobinPartitioner{}

	choice := partitioner.Partition(nil, 1)
	if choice != 0 {
		t.Error("Returned non-zero partition when only one available.")
	}

	for i := 1; i < 50; i++ {
		choice := partitioner.Partition(nil, 7)
		if choice != i%7 {
			t.Fatal("Returned partition", choice, "expecting", i%7)
		}
	}
}

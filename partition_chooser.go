package kafka

import "math/rand"

type PartitionChooser interface {
	ChoosePartition(key Encoder, partitions int) int
}

type RandomPartitioner struct {
}

func (p RandomPartitioner) ChoosePartition(key Encoder, partitions int) int {
	return rand.Intn(partitions)
}

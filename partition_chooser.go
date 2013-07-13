package kafka

import "math/rand"

type PartitionChooser interface {
	ChoosePartition(key encoder, partitions int) int
}

type RandomPartitioner struct {
}

func (p RandomPartitioner) ChoosePartition(key encoder, partitions int) int {
	return rand.Intn(partitions)
}

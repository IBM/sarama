package kafka

import "math/rand"

type PartitionChooser interface {
	ChoosePartition(key encoder, partitions []int32) int32
}

type RandomPartitioner struct {
}

func (p RandomPartitioner) ChoosePartition(key encoder, partitions []int32) int32 {
	return partitions[rand.Intn(len(partitions))]
}

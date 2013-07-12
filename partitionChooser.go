package kafka

import "math/rand"

type partitionChooser interface {
	choosePartition(key encoder, partitions []int32) int32
}

type RandomPartitioner struct {
}

func (p RandomPartitioner) choosePartition(key encoder, partitions []int32) int32 {
	return partitions[rand.Intn(len(partitions))]
}

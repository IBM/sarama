package kafka

import "math/rand"

type partitionChooser interface {
	choosePartition(options []int32) int32
}

type randomPartitioner struct {
}

func (p randomPartitioner) choosePartition(options []int32) int32 {
	return options[rand.Intn(len(options))]
}

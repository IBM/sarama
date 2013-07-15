package kafka

import "math/rand"

type Partitioner interface {
	Partition(key Encoder, numPartitions int) int
}

type RandomPartitioner struct {
}

func (p RandomPartitioner) Partition(key Encoder, numPartitions int) int {
	return rand.Intn(numPartitions)
}

package sarama

import "math/rand"

// Partitioner is anything that, given a Kafka message key and a number of partitions indexed [0...numPartitions-1],
// decides to which partition to send the message. RandomPartitioner and RoundRobinPartitioner are the
// two simple default implementations.
type Partitioner interface {
	Partition(key Encoder, numPartitions int) int
}

// RandomPartitioner implements the Partitioner interface by choosing a random partition each time.
type RandomPartitioner struct {
}

func (p RandomPartitioner) Partition(key Encoder, numPartitions int) int {
	return rand.Intn(numPartitions)
}

// RoundRobinPartitioner implements the Partitioner interface by walking through the available partitions one at a time.
type RoundRobinPartitioner struct {
	partition int
}

func (p *RoundRobinPartitioner) Partition(key Encoder, numPartitions int) int {
	if p.partition >= numPartitions {
		p.partition = 0
	}
	ret := p.partition
	p.partition++
	return ret
}

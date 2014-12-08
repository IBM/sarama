package sarama

import (
	"hash"
	"hash/fnv"
	"math/rand"
	"time"
)

// Partitioner is anything that, given a Kafka message key and a number of partitions indexed [0...numPartitions-1],
// decides to which partition to send the message. RandomPartitioner, RoundRobinPartitioner and HashPartitioner are provided
// as simple default implementations.
type Partitioner interface {
	Partition(key Encoder, numPartitions int32) (int32, error) // Partition takes the key and partition count and chooses a partition

	// RequiresConsistency indicates to the user of the partitioner whether the mapping of key->partition is consistent or not.
	// Specifically, if a partitioner requires consistency then it must be allowed to choose from all partitions (even ones known to
	// be unavailable), and its choice must be respected by the caller. The obvious example is the HashPartitioner.
	RequiresConsistency() bool
}

// PartitionerConstructor is the type for a function capable of constructing new Partitioners.
type PartitionerConstructor func() Partitioner

// RandomPartitioner implements the Partitioner interface by choosing a random partition each time.
type RandomPartitioner struct {
	generator *rand.Rand
}

func NewRandomPartitioner() Partitioner {
	p := new(RandomPartitioner)
	p.generator = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	return p
}

func (p *RandomPartitioner) Partition(key Encoder, numPartitions int32) (int32, error) {
	return int32(p.generator.Intn(int(numPartitions))), nil
}

func (p *RandomPartitioner) RequiresConsistency() bool {
	return false
}

// RoundRobinPartitioner implements the Partitioner interface by walking through the available partitions one at a time.
type RoundRobinPartitioner struct {
	partition int32
}

func NewRoundRobinPartitioner() Partitioner {
	return &RoundRobinPartitioner{}
}

func (p *RoundRobinPartitioner) Partition(key Encoder, numPartitions int32) (int32, error) {
	if p.partition >= numPartitions {
		p.partition = 0
	}
	ret := p.partition
	p.partition++
	return ret, nil
}

func (p *RoundRobinPartitioner) RequiresConsistency() bool {
	return false
}

// HashPartitioner implements the Partitioner interface. If the key is nil, or fails to encode, then a random partition
// is chosen. Otherwise the FNV-1a hash of the encoded bytes is used modulus the number of partitions. This ensures that messages
// with the same key always end up on the same partition.
type HashPartitioner struct {
	random Partitioner
	hasher hash.Hash32
}

func NewHashPartitioner() Partitioner {
	p := new(HashPartitioner)
	p.random = NewRandomPartitioner()
	p.hasher = fnv.New32a()
	return p
}

func (p *HashPartitioner) Partition(key Encoder, numPartitions int32) (int32, error) {
	if key == nil {
		return p.random.Partition(key, numPartitions)
	}
	bytes, err := key.Encode()
	if err != nil {
		return -1, err
	}
	p.hasher.Reset()
	_, err = p.hasher.Write(bytes)
	if err != nil {
		return -1, err
	}
	hash := int32(p.hasher.Sum32())
	if hash < 0 {
		hash = -hash
	}
	return hash % numPartitions, nil
}

func (p *HashPartitioner) RequiresConsistency() bool {
	return true
}

// ConstantPartitioner implements the Partitioner interface by just returning a constant value.
type ConstantPartitioner struct {
	Constant int32
}

func (p *ConstantPartitioner) Partition(key Encoder, numPartitions int32) (int32, error) {
	return p.Constant, nil
}

func (p *ConstantPartitioner) RequiresConsistency() bool {
	return true
}

package sarama

import (
	"hash"
	"hash/fnv"
	"math/rand"
	"time"
)

// Partitioner is anything that, given a Kafka message and a number of partitions indexed [0...numPartitions-1],
// decides to which partition to send the message. RandomPartitioner, RoundRobinPartitioner and HashPartitioner are provided
// as simple default implementations.
type Partitioner interface {
	// Partition takes a message and partition count and chooses a partition
	Partition(message *ProducerMessage, numPartitions int32) (int32, error)

	// RequiresConsistency indicates to the user of the partitioner whether the
	// mapping of key->partition is consistent or not. Specifically, if a
	// partitioner requires consistency then it must be allowed to choose from all
	// partitions (even ones known to be unavailable), and its choice must be
	// respected by the caller. The obvious example is the HashPartitioner.
	RequiresConsistency() bool
}

// DynamicConsistencyPartitioner can optionally be implemented by Partitioners
// in order to allow more flexibility than is originally allowed by the
// RequiresConsistency method in the Partitioner interface. This allows
// partitioners to require consistency sometimes, but not all times. It's useful
// for, e.g., the HashPartitioner, which does not require consistency if the
// message key is nil.
type DynamicConsistencyPartitioner interface {
	Partitioner

	// MessageRequiresConsistency is similar to Partitioner.RequiresConsistency,
	// but takes in the message being partitioned so that the partitioner can
	// make a per-message determination.
	MessageRequiresConsistency(message *ProducerMessage) bool
}

// PartitionerConstructor is the type for a function capable of constructing new Partitioners.
type PartitionerConstructor func(topic string) Partitioner

type manualPartitioner struct{}

// HashPartitionOption lets you modify default values of the partitioner
type HashPartitionerOption func(*hashPartitioner)

// WithAbsFirst means that the partitioner handles absolute values
// in the same way as the reference Java implementation
func WithAbsFirst() HashPartitionerOption {
	return func(hp *hashPartitioner) {
		hp.referenceAbs = true
	}
}

// WithCustomHashFunction lets you specify what hash function to use for the partitioning
func WithCustomHashFunction(hasher func() hash.Hash32) HashPartitionerOption {
	return func(hp *hashPartitioner) {
		hp.hasher = hasher
	}
}

// WithCustomFallbackPartitioner lets you specify what HashPartitioner should be used in case a Distribution Key is empty
func WithCustomFallbackPartitioner(randomHP *hashPartitioner) HashPartitionerOption {
	return func(hp *hashPartitioner) {
		hp.random = hp
	}
}

// NewManualPartitioner returns a Partitioner which uses the partition manually set in the provided
// ProducerMessage's Partition field as the partition to produce to.
func NewManualPartitioner(topic string) Partitioner {
	return new(manualPartitioner)
}

func (p *manualPartitioner) Partition(message *ProducerMessage, numPartitions int32) (int32, error) {
	return message.Partition, nil
}

func (p *manualPartitioner) RequiresConsistency() bool {
	return true
}

type randomPartitioner struct {
	generator *rand.Rand
}

// NewRandomPartitioner returns a Partitioner which chooses a random partition each time.
func NewRandomPartitioner(topic string) Partitioner {
	p := new(randomPartitioner)
	p.generator = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	return p
}

func (p *randomPartitioner) Partition(message *ProducerMessage, numPartitions int32) (int32, error) {
	return int32(p.generator.Intn(int(numPartitions))), nil
}

func (p *randomPartitioner) RequiresConsistency() bool {
	return false
}

type roundRobinPartitioner struct {
	partition int32
}

// NewRoundRobinPartitioner returns a Partitioner which walks through the available partitions one at a time.
func NewRoundRobinPartitioner(topic string) Partitioner {
	return &roundRobinPartitioner{}
}

func (p *roundRobinPartitioner) Partition(message *ProducerMessage, numPartitions int32) (int32, error) {
	if p.partition >= numPartitions {
		p.partition = 0
	}
	ret := p.partition
	p.partition++
	return ret, nil
}

func (p *roundRobinPartitioner) RequiresConsistency() bool {
	return false
}

type hashPartitioner struct {
	random       Partitioner
	hasher       func() hash.Hash32
	referenceAbs bool
}

// NewCustomHashPartitioner is a wrapper around NewHashPartitioner, allowing the use of custom hasher.
// The argument is a function providing the instance, implementing the hash.Hash32 interface. This is to ensure that
// each partition dispatcher gets its own hasher, to avoid concurrency issues by sharing an instance.
func NewCustomHashPartitioner(hasher func() hash.Hash32) PartitionerConstructor {
	return func(topic string) Partitioner {
		p := new(hashPartitioner)
		p.random = NewRandomPartitioner(topic)
		p.hasher = hasher
		p.referenceAbs = false
		return p
	}
}

// NewCustomPartitioner creates a default Partitioner but lets you specify the behavior of each component via options
func NewCustomPartitioner(options ...HashPartitionerOption) PartitionerConstructor {
	return func(topic string) Partitioner {
		p := new(hashPartitioner)
		p.random = NewRandomPartitioner(topic)
		p.hasher = fnv.New32a
		p.referenceAbs = false
		for _, option := range options {
			option(p)
		}
		return p
	}
}

// NewHashPartitioner returns a Partitioner which behaves as follows. If the message's key is nil then a
// random partition is chosen. Otherwise the FNV-1a hash of the encoded bytes of the message key is used,
// modulus the number of partitions. This ensures that messages with the same key always end up on the
// same partition.
func NewHashPartitioner(topic string) Partitioner {
	p := new(hashPartitioner)
	p.random = NewRandomPartitioner(topic)
	p.hasher = fnv.New32a
	p.referenceAbs = false
	return p
}

// NewReferenceHashPartitioner is like NewHashPartitioner except that it handles absolute values
// in the same way as the reference Java implementation. NewHashPartitioner was supposed to do
// that but it had a mistake and now there are people depending on both behaviours. This will
// all go away on the next major version bump.
func NewReferenceHashPartitioner(topic string) Partitioner {
	p := new(hashPartitioner)
	p.random = NewRandomPartitioner(topic)
	p.hasher = fnv.New32a
	p.referenceAbs = true
	return p
}

func (p *hashPartitioner) Partition(message *ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return p.random.Partition(message, numPartitions)
	}
	bytes, err := message.Key.Encode()
	if err != nil {
		return -1, err
	}
	hasher := p.hasher()
	_, err = hasher.Write(bytes)
	if err != nil {
		return -1, err
	}
	var partition int32
	// Turns out we were doing our absolute value in a subtly different way from the upstream
	// implementation, but now we need to maintain backwards compat for people who started using
	// the old version; if referenceAbs is set we are compatible with the reference java client
	// but not past Sarama versions
	if p.referenceAbs {
		partition = (int32(hasher.Sum32()) & 0x7fffffff) % numPartitions
	} else {
		partition = int32(hasher.Sum32()) % numPartitions
		if partition < 0 {
			partition = -partition
		}
	}
	return partition, nil
}

func (p *hashPartitioner) RequiresConsistency() bool {
	return true
}

func (p *hashPartitioner) MessageRequiresConsistency(message *ProducerMessage) bool {
	return message.Key != nil
}

type consistentHashPartitioner struct {
	random Partitioner
}

// Creates a Partitioner that uses go-jump for consistent key->partition hashing
func NewConsistentHashPartitioner(topic string) Partitioner {
	p := new(consistentHashPartitioner)
	p.random = NewRandomPartitioner(topic)
	return p
}

func (p *consistentHashPartitioner) Partition(message *ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return p.random.Partition(message, numPartitions)
	}
	bytes, err := message.Key.Encode()
	if err != nil {
		return -1, err
	}
	hasher := fnv.New64a()
	_, err = hasher.Write(bytes)
	if err != nil {
		return -1, err
	}
	partition := jumpHash(hasher.Sum64(), int(numPartitions))
	if partition < 0 {
		partition = -partition
	}
	return partition, nil
}

func (p *consistentHashPartitioner) RequiresConsistency() bool {
	return true
}

/*
Implements Google's Jump Consistent Hash
Copied from: https://github.com/dgryski/go-jump
From the paper "A Fast, Minimal Memory, Consistent Hash Algorithm" by John Lamping, Eric Veach (2014).
http://arxiv.org/abs/1406.2294
*/
// Hash consistently chooses a hash bucket number in the range [0, numBuckets) for the given key. numBuckets must be >= 1.
func jumpHash(key uint64, numBuckets int) int32 {

	var b int64 = -1
	var j int64

	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int32(b)
}

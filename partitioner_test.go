//go:build !functional

package sarama

import (
	"crypto/rand"
	"hash/crc32"
	"hash/fnv"
	"log"
	"testing"
)

func assertPartitioningConsistent(t *testing.T, partitioner Partitioner, message *ProducerMessage, numPartitions int32) {
	choice, err := partitioner.Partition(message, numPartitions)
	if err != nil {
		t.Error(partitioner, err)
	}
	if choice < 0 || choice >= numPartitions {
		t.Error(partitioner, "returned partition", choice, "outside of range for", message)
	}
	for i := 1; i < 50; i++ {
		newChoice, err := partitioner.Partition(message, numPartitions)
		if err != nil {
			t.Error(partitioner, err)
		}
		if newChoice != choice {
			t.Error(partitioner, "returned partition", newChoice, "inconsistent with", choice, ".")
		}
	}
}

type partitionerTestCase struct {
	key               string
	expectedPartition int32
}

func partitionAndAssert(t *testing.T, partitioner Partitioner, numPartitions int32, testCase partitionerTestCase) {
	t.Run("partitionAndAssert "+testCase.key, func(t *testing.T) {
		msg := &ProducerMessage{
			Key: StringEncoder(testCase.key),
		}

		partition, err := partitioner.Partition(msg, numPartitions)

		if err != nil {
			t.Error(partitioner, err)
		}
		if partition != testCase.expectedPartition {
			t.Error(partitioner, "partitioning", testCase.key, "returned partition", partition, "but expected", testCase.expectedPartition, ".")
		}
	})
}

func TestRandomPartitioner(t *testing.T) {
	partitioner := NewRandomPartitioner("mytopic")

	choice, err := partitioner.Partition(nil, 1)
	if err != nil {
		t.Error(partitioner, err)
	}
	if choice != 0 {
		t.Error("Returned non-zero partition when only one available.")
	}

	for i := 1; i < 50; i++ {
		choice, err := partitioner.Partition(nil, 50)
		if err != nil {
			t.Error(partitioner, err)
		}
		if choice < 0 || choice >= 50 {
			t.Error("Returned partition", choice, "outside of range.")
		}
	}
}

func TestRoundRobinPartitioner(t *testing.T) {
	partitioner := NewRoundRobinPartitioner("mytopic")

	choice, err := partitioner.Partition(nil, 1)
	if err != nil {
		t.Error(partitioner, err)
	}
	if choice != 0 {
		t.Error("Returned non-zero partition when only one available.")
	}

	var i int32
	for i = 1; i < 50; i++ {
		choice, err := partitioner.Partition(nil, 7)
		if err != nil {
			t.Error(partitioner, err)
		}
		if choice != i%7 {
			t.Error("Returned partition", choice, "expecting", i%7)
		}
	}
}

func TestNewHashPartitionerWithHasher(t *testing.T) {
	// use the current default hasher fnv.New32a()
	partitioner := NewCustomHashPartitioner(fnv.New32a)("mytopic")

	choice, err := partitioner.Partition(&ProducerMessage{}, 1)
	if err != nil {
		t.Error(partitioner, err)
	}
	if choice != 0 {
		t.Error("Returned non-zero partition when only one available.")
	}

	for i := 1; i < 50; i++ {
		choice, err := partitioner.Partition(&ProducerMessage{}, 50)
		if err != nil {
			t.Error(partitioner, err)
		}
		if choice < 0 || choice >= 50 {
			t.Error("Returned partition", choice, "outside of range for nil key.")
		}
	}

	buf := make([]byte, 256)
	for i := 1; i < 50; i++ {
		if _, err := rand.Read(buf); err != nil {
			t.Error(err)
		}
		assertPartitioningConsistent(t, partitioner, &ProducerMessage{Key: ByteEncoder(buf)}, 50)
	}
}

func TestHashPartitionerWithHasherMinInt32(t *testing.T) {
	// use the current default hasher fnv.New32a()
	partitioner := NewCustomHashPartitioner(fnv.New32a)("mytopic")

	msg := ProducerMessage{}
	// "1468509572224" generates 2147483648 (uint32) result from Sum32 function
	// which is -2147483648 or int32's min value
	msg.Key = StringEncoder("1468509572224")

	choice, err := partitioner.Partition(&msg, 50)
	if err != nil {
		t.Error(partitioner, err)
	}
	if choice < 0 || choice >= 50 {
		t.Error("Returned partition", choice, "outside of range for nil key.")
	}
}

func TestHashPartitioner(t *testing.T) {
	partitioner := NewHashPartitioner("mytopic")

	choice, err := partitioner.Partition(&ProducerMessage{}, 1)
	if err != nil {
		t.Error(partitioner, err)
	}
	if choice != 0 {
		t.Error("Returned non-zero partition when only one available.")
	}

	for i := 1; i < 50; i++ {
		choice, err := partitioner.Partition(&ProducerMessage{}, 50)
		if err != nil {
			t.Error(partitioner, err)
		}
		if choice < 0 || choice >= 50 {
			t.Error("Returned partition", choice, "outside of range for nil key.")
		}
	}

	buf := make([]byte, 256)
	for i := 1; i < 50; i++ {
		if _, err := rand.Read(buf); err != nil {
			t.Error(err)
		}
		assertPartitioningConsistent(t, partitioner, &ProducerMessage{Key: ByteEncoder(buf)}, 50)
	}
}

func TestHashPartitionerConsistency(t *testing.T) {
	partitioner := NewHashPartitioner("mytopic")
	ep, ok := partitioner.(DynamicConsistencyPartitioner)

	if !ok {
		t.Error("Hash partitioner does not implement DynamicConsistencyPartitioner")
	}

	consistency := ep.MessageRequiresConsistency(&ProducerMessage{Key: StringEncoder("hi")})
	if !consistency {
		t.Error("Messages with keys should require consistency")
	}
	consistency = ep.MessageRequiresConsistency(&ProducerMessage{})
	if consistency {
		t.Error("Messages without keys should require consistency")
	}
}

func TestHashPartitionerMinInt32(t *testing.T) {
	partitioner := NewHashPartitioner("mytopic")

	msg := ProducerMessage{}
	// "1468509572224" generates 2147483648 (uint32) result from Sum32 function
	// which is -2147483648 or int32's min value
	msg.Key = StringEncoder("1468509572224")

	choice, err := partitioner.Partition(&msg, 50)
	if err != nil {
		t.Error(partitioner, err)
	}
	if choice < 0 || choice >= 50 {
		t.Error("Returned partition", choice, "outside of range for nil key.")
	}
}

func TestConsistentCRCHashPartitioner(t *testing.T) {
	numPartitions := int32(100)
	partitioner := NewConsistentCRCHashPartitioner("mytopic")

	testCases := []partitionerTestCase{
		{
			key:               "abc123def456",
			expectedPartition: 57,
		},
		{
			// `SheetJS` has a crc32 hash value of 2647669026 (which is -1647298270 as a signed int32)
			// Modding the signed value will give a partition of 70.  Modding the unsigned value will give 26
			key:               "SheetJS",
			expectedPartition: 26,
		},
		{
			key:               "9e8c7f4cf45857cfff7645d6",
			expectedPartition: 24,
		},
		{
			key:               "3900446192ff85a5f67da10c",
			expectedPartition: 75,
		},
		{
			key:               "0f4407b7a67d6d27de372198",
			expectedPartition: 50,
		},
	}

	for _, tc := range testCases {
		partitionAndAssert(t, partitioner, numPartitions, tc)
	}
}

func TestCustomPartitionerWithConsistentHashing(t *testing.T) {
	// Setting both `hashUnsigned` and the hash function to `crc32.NewIEEE` is equivalent to using `NewConsistentCRCHashPartitioner`
	partitioner := NewCustomPartitioner(
		WithHashUnsigned(),
		WithCustomHashFunction(crc32.NewIEEE),
	)("mytopic")

	// See above re: why `SheetJS`
	msg := ProducerMessage{
		Key: StringEncoder("SheetJS"),
	}

	choice, err := partitioner.Partition(&msg, 100)
	if err != nil {
		t.Error(partitioner, err)
	}
	expectedPartition := int32(26)
	if choice != expectedPartition {
		t.Error(partitioner, "returned partition", choice, "but expected", expectedPartition, ".")
	}
}

func TestManualPartitioner(t *testing.T) {
	partitioner := NewManualPartitioner("mytopic")

	choice, err := partitioner.Partition(&ProducerMessage{}, 1)
	if err != nil {
		t.Error(partitioner, err)
	}
	if choice != 0 {
		t.Error("Returned non-zero partition when only one available.")
	}

	for i := int32(1); i < 50; i++ {
		choice, err := partitioner.Partition(&ProducerMessage{Partition: i}, 50)
		if err != nil {
			t.Error(partitioner, err)
		}
		if choice != i {
			t.Error("Returned partition not the same as the input partition")
		}
	}
}

func TestWithCustomFallbackPartitioner(t *testing.T) {
	topic := "mytopic"

	partitioner := NewCustomPartitioner(
		// override default random partitioner with round robin
		WithCustomFallbackPartitioner(NewRoundRobinPartitioner(topic)),
	)(topic)

	// Should use round robin implementation if there is no key
	var i int32
	for i = 0; i < 50; i++ {
		choice, err := partitioner.Partition(&ProducerMessage{Key: nil}, 7)
		if err != nil {
			t.Error(partitioner, err)
		}
		if choice != i%7 {
			t.Error("Returned partition", choice, "expecting", i%7)
		}
	}

	// should use hash partitioner if key is specified
	buf := make([]byte, 256)
	for i := 0; i < 50; i++ {
		if _, err := rand.Read(buf); err != nil {
			t.Error(err)
		}
		assertPartitioningConsistent(t, partitioner, &ProducerMessage{Key: ByteEncoder(buf)}, 50)
	}
}

// By default, Sarama uses the message's key to consistently assign a partition to
// a message using hashing. If no key is set, a random partition will be chosen.
// This example shows how you can partition messages randomly, even when a key is set,
// by overriding Config.Producer.Partitioner.
func ExamplePartitioner_random() {
	config := NewTestConfig()
	config.Producer.Partitioner = NewRandomPartitioner

	producer, err := NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Println(err)
		return
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Println("Failed to close producer:", err)
		}
	}()

	msg := &ProducerMessage{Topic: "test", Key: StringEncoder("key is set"), Value: StringEncoder("test")}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Println("Failed to produce message to kafka cluster.")
		return
	}

	log.Printf("Produced message to partition %d with offset %d", partition, offset)
}

// This example shows how to assign partitions to your messages manually.
func ExamplePartitioner_manual() {
	config := NewTestConfig()

	// First, we tell the producer that we are going to partition ourselves.
	config.Producer.Partitioner = NewManualPartitioner

	producer, err := NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Println(err)
		return
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Println("Failed to close producer:", err)
		}
	}()

	// Now, we set the Partition field of the ProducerMessage struct.
	msg := &ProducerMessage{Topic: "test", Partition: 6, Value: StringEncoder("test")}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Println("Failed to produce message to kafka cluster.")
		return
	}

	if partition != 6 {
		log.Println("Message should have been produced to partition 6!")
		return
	}

	log.Printf("Produced message to partition %d with offset %d", partition, offset)
}

// This example shows how to set a different partitioner depending on the topic.
func ExamplePartitioner_per_topic() {
	config := NewTestConfig()
	config.Producer.Partitioner = func(topic string) Partitioner {
		switch topic {
		case "access_log", "error_log":
			return NewRandomPartitioner(topic)

		default:
			return NewHashPartitioner(topic)
		}
	}

	// ...
}

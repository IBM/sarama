package cluster

import (
	"sort"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PartitionSlice", func() {

	It("should sort correctly", func() {
		p1 := Partition{Addr: "host1:9093", Id: 1}
		p2 := Partition{Addr: "host1:9092", Id: 2}
		p3 := Partition{Addr: "host2:9092", Id: 3}
		p4 := Partition{Addr: "host2:9093", Id: 4}
		p5 := Partition{Addr: "host1:9092", Id: 5}

		slice := PartitionSlice{p1, p2, p3, p4, p5}
		sort.Sort(slice)
		Expect(slice).To(BeEquivalentTo(PartitionSlice{p2, p5, p1, p3, p4}))
	})

})

/*********************************************************************
 * TEST HOOK
 *********************************************************************/

func checkOrFail(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)

	client, err := sarama.NewClient("sarama-cluster-client", []string{"127.0.0.1:29092"}, &sarama.ClientConfig{
		MetadataRetries: 30,
		WaitForElection: time.Second,
	})
	checkOrFail(t, err)
	defer client.Close()

	producer, err := sarama.NewProducer(client, &sarama.ProducerConfig{
		Partitioner:      sarama.NewHashPartitioner(),
		MaxBufferedBytes: 1024 * 1024,
		MaxBufferTime:    1000,
	})
	checkOrFail(t, err)
	defer producer.Close()

	for i := 0; i < 10000; i++ {
		checkOrFail(t, producer.SendMessage(tnT, nil, sarama.ByteEncoder([]byte("PLAINDATA"))))
	}
	RunSpecs(t, "sarama/cluster")
}

/*******************************************************************
 * TEST HELPERS
 *******************************************************************/

var tnG = "sarama-cluster-group"
var tnT = "sarama-cluster-topic"

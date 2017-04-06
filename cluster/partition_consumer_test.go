package cluster

import (
	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PartitionConsumer", func() {
	var subject *PartitionConsumer
	var stream *mockStream

	BeforeEach(func() {
		stream = newMockStream()
		subject = &PartitionConsumer{partition: 3, topic: tnT, stream: stream}
	})

	It("should fetch batches of events (if available)", func() {
		Expect(subject.Fetch()).To(BeNil())
		stream.events <- &sarama.ConsumerEvent{}
		stream.events <- &sarama.ConsumerEvent{}
		batch := subject.Fetch()
		Expect(batch).NotTo(BeNil())
		Expect(batch.Topic).To(Equal(tnT))
		Expect(batch.Partition).To(Equal(int32(3)))
		Expect(batch.Events).To(HaveLen(2))
		Expect(subject.Fetch()).To(BeNil())
	})

	It("should close consumers", func() {
		Expect(subject.Close()).To(BeNil())
		Expect(stream.closed).To(BeTrue())
	})

})

/********************************************************************
 * TEST HOOK
 *********************************************************************/

type mockStream struct {
	closed bool
	events chan *sarama.ConsumerEvent
}

func newMockStream() *mockStream                           { return &mockStream{events: make(chan *sarama.ConsumerEvent, 1000)} }
func (m *mockStream) Events() <-chan *sarama.ConsumerEvent { return m.events }
func (m *mockStream) Close() error                         { m.closed = true; return nil }

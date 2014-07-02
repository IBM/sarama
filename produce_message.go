package sarama

import (
	"log"
	"time"
)

type produceMessage struct {
	tp         topicPartition
	key, value []byte
	retried    bool
	sync       bool
}

type produceRequestBuilder []*produceMessage

// If the message is synchronous, we manually send it and wait for a return.
// Otherwise, we just hand it back to the producer to enqueue using the normal
// method.
func (msg *produceMessage) enqueue(p *Producer) error {
	if !msg.sync {
		return p.addMessage(msg)
	}

	var prb produceRequestBuilder = []*produceMessage{msg}
	bp, err := p.brokerProducerFor(msg.tp)
	if err != nil {
		return err
	}
	errs := make(chan error, 1)
	bp.flushRequest(p, prb, func(err error) {
		errs <- err
		close(errs)
	})
	return <-errs

}

func (msg *produceMessage) reenqueue(p *Producer) error {
	if msg.retried {
		return DroppedMessagesError{}
	}
	msg.retried = true
	return msg.enqueue(p)
}

func (msg *produceMessage) hasTopicPartition(topic string, partition int32) bool {
	return msg.tp.partition == partition && msg.tp.topic == topic
}

func (b produceRequestBuilder) toRequest(config *ProducerConfig) *ProduceRequest {
	req := &ProduceRequest{RequiredAcks: config.RequiredAcks, Timeout: int32(config.Timeout / time.Millisecond)}

	// If compression is enabled, we need to group messages by topic-partition and
	// wrap them in MessageSets. We already discarded that grouping, so we
	// inefficiently re-sort them. This could be optimized (ie. pass a hash around
	// rather than an array. Not sure what the best way is.
	if config.Compression != CompressionNone {
		msgSets := make(map[topicPartition]*MessageSet)
		for _, pmsg := range b {
			msgSet, ok := msgSets[pmsg.tp]
			if !ok {
				msgSet = new(MessageSet)
				msgSets[pmsg.tp] = msgSet
			}

			msgSet.addMessage(&Message{Codec: CompressionNone, Key: pmsg.key, Value: pmsg.value})
		}
		for tp, msgSet := range msgSets {
			valBytes, err := encode(msgSet)
			if err != nil {
				log.Fatal(err) // if this happens, it's basically our fault.
			}
			msg := Message{Codec: config.Compression, Key: nil, Value: valBytes}
			req.AddMessage(tp.topic, tp.partition, &msg)
		}
		return req
	}

	// Compression is not enabled. Dumb-ly append each request directly to the
	// request, with no MessageSet wrapper.
	for _, pmsg := range b {
		msg := Message{Codec: config.Compression, Key: pmsg.key, Value: pmsg.value}
		req.AddMessage(pmsg.tp.topic, pmsg.tp.partition, &msg)
	}
	return req
}

func (msg *produceMessage) byteSize() uint32 {
	return uint32(len(msg.key) + len(msg.value))
}

func (b produceRequestBuilder) byteSize() uint32 {
	var size uint32
	for _, m := range b {
		size += m.byteSize()
	}
	return size
}

func (b produceRequestBuilder) reverseEach(fn func(m *produceMessage)) {
	for i := len(b) - 1; i >= 0; i-- {
		fn(b[i])
	}
}

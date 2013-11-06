package sarama

type produceMessage struct {
	tp         topicPartition
	key, value []byte
	failures   uint32
}

type produceRequestBuilder []*produceMessage

func (msg *produceMessage) reenqueue(p *Producer) (ok bool) {
	if msg.failures < p.config.MaxDeliveryRetries {
		msg.failures++
		p.addMessage(msg)
		return true
	} else {
		return false
	}
}

func (msg *produceMessage) hasTopicPartition(topic string, partition int32) bool {
	return msg.tp.partition == partition && msg.tp.topic == topic
}

func (b produceRequestBuilder) toRequest(config *ProducerConfig) *ProduceRequest {
	req := &ProduceRequest{RequiredAcks: config.RequiredAcks, Timeout: config.Timeout}
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

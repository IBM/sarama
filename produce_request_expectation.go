package sarama

import (
	"bytes"
	"encoding/binary"
)

type ProduceRequestExpectation struct {
	err             error
	topicPartitions []produceRequestTP
}

type produceRequestTP struct {
	topic     string
	partition int32
	nMessages int
	err       KError
}

func (e *ProduceRequestExpectation) AddTopicPartition(
	topic string, partition int32, nMessages int, err KError,
) *ProduceRequestExpectation {
	prtp := produceRequestTP{topic, partition, nMessages, err}
	e.topicPartitions = append(e.topicPartitions, prtp)
	return e
}

func (b *MockBroker) ExpectProduceRequest() *ProduceRequestExpectation {
	e := &ProduceRequestExpectation{}
	b.expectations <- e
	return e
}

func (e *ProduceRequestExpectation) ResponseBytes() []byte {
	buf := new(bytes.Buffer)

	byTopic := make(map[string][]produceRequestTP)
	for _, prtp := range e.topicPartitions {
		byTopic[prtp.topic] = append(byTopic[prtp.topic], prtp)
	}

	binary.Write(buf, binary.BigEndian, uint32(len(byTopic)))
	for topic, tps := range byTopic {
		binary.Write(buf, binary.BigEndian, uint16(len(topic)))
		buf.Write([]byte(topic)) // TODO: Does this write a null?
		binary.Write(buf, binary.BigEndian, uint32(len(tps)))
		for _, tp := range tps {
			binary.Write(buf, binary.BigEndian, uint32(tp.partition))
			binary.Write(buf, binary.BigEndian, uint16(tp.err)) // TODO: error
			binary.Write(buf, binary.BigEndian, uint64(0))      // offset
		}
	}

	/*
	   sample response:

	   0x00, 0x00, 0x00, 0x02, // 0:3 number of topics

	   0x00, 0x07, 't', 'o', 'p', 'i', 'c', '_', 'b', // 4:12 topic name
	   0x00, 0x00, 0x00, 0x01, // 13:16 number of blocks for this topic
	   0x00, 0x00, 0x00, 0x00, // 17:20 partition id
	   0x00, 0x00, // 21:22 error: 0 means no error
	   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 23:30 offset

	   0x00, 0x07, 't', 'o', 'p', 'i', 'c', '_', 'c', // 4:12 topic name
	   0x00, 0x00, 0x00, 0x01, // 13:16 number of blocks for this topic
	   0x00, 0x00, 0x00, 0x00, // 17:20 partition id
	   0x00, 0x00, // 21:22 error: 0 means no error
	   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 23:30 offset
	*/
	return buf.Bytes()
}

func (e *ProduceRequestExpectation) Error(err error) *ProduceRequestExpectation {
	e.err = err
	return e
}

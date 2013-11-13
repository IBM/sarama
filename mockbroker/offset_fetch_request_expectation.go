package mockbroker

import (
	"bytes"
	"encoding/binary"
)

type OffsetFetchRequestExpectation struct {
	topicPartitions []offsetFetchRequestTP
}

type offsetFetchRequestTP struct {
	topic     string
	partition int32
	offset    uint64
}

func (e *OffsetFetchRequestExpectation) AddTopicPartition(
	topic string, partition int32, offset uint64,
) *OffsetFetchRequestExpectation {
	ofrtp := offsetFetchRequestTP{topic, partition, offset}
	e.topicPartitions = append(e.topicPartitions, ofrtp)
	return e
}

func (b *MockBroker) ExpectOffsetFetchRequest() *OffsetFetchRequestExpectation {
	e := &OffsetFetchRequestExpectation{}
	b.expectations <- e
	return e
}

func (e *OffsetFetchRequestExpectation) ResponseBytes() []byte {
	buf := new(bytes.Buffer)

	byTopic := make(map[string][]offsetFetchRequestTP)
	for _, ofrtp := range e.topicPartitions {
		byTopic[ofrtp.topic] = append(byTopic[ofrtp.topic], ofrtp)
	}

	binary.Write(buf, binary.BigEndian, uint32(len(byTopic)))
	for topic, tps := range byTopic {
		binary.Write(buf, binary.BigEndian, uint16(len(topic)))
		buf.Write([]byte(topic))
		binary.Write(buf, binary.BigEndian, uint32(len(tps)))
		for _, tp := range tps {
			binary.Write(buf, binary.BigEndian, uint32(tp.partition))
			binary.Write(buf, binary.BigEndian, uint16(0)) // error
			binary.Write(buf, binary.BigEndian, uint32(1))
			binary.Write(buf, binary.BigEndian, uint64(tp.offset)) // offset
		}
	}

	/*
		sample response:

		0x00, 0x00, 0x00, 0x01, // number of topics
		0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c', // topic name
		0x00, 0x00, 0x00, 0x01, // number of blocks for this partition
		0x00, 0x00, 0x00, 0x00, // partition id
		0x00, 0x00, // error
		0x00, 0x00, 0x00, 0x01, // number of offsets
		0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x01, // offset
	*/
	return buf.Bytes()
}

package sarama

import (
	"bytes"
	"encoding/binary"
)

type MetadataRequestExpectation struct {
	err             error
	topicPartitions []metadataRequestTP
	brokers         []mockbrokerish
}

type metadataRequestTP struct {
	topic     string
	partition int32
	brokerId  int
}

type mockbrokerish interface {
	BrokerID() int
	Port() int32
}

func (e *MetadataRequestExpectation) AddBroker(b mockbrokerish) *MetadataRequestExpectation {
	e.brokers = append(e.brokers, b)
	return e
}

func (e *MetadataRequestExpectation) AddTopicPartition(
	topic string, partition int32, brokerId int,
) *MetadataRequestExpectation {
	mrtp := metadataRequestTP{topic, partition, brokerId}
	e.topicPartitions = append(e.topicPartitions, mrtp)
	return e
}

func (e *MetadataRequestExpectation) Error(err error) *MetadataRequestExpectation {
	e.err = err
	return e
}

func (e *MetadataRequestExpectation) ResponseBytes() []byte {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, uint32(len(e.brokers)))
	for _, broker := range e.brokers {
		binary.Write(buf, binary.BigEndian, uint32(broker.BrokerID()))
		buf.Write([]byte{0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't'})
		binary.Write(buf, binary.BigEndian, uint32(broker.Port()))
	}

	byTopic := make(map[string][]metadataRequestTP)
	for _, mrtp := range e.topicPartitions {
		byTopic[mrtp.topic] = append(byTopic[mrtp.topic], mrtp)
	}

	binary.Write(buf, binary.BigEndian, uint32(len(byTopic)))
	for topic, tps := range byTopic {
		// we don't support mocking errors at topic-level; only partition-level
		binary.Write(buf, binary.BigEndian, uint16(0))
		binary.Write(buf, binary.BigEndian, uint16(len(topic)))
		buf.Write([]byte(topic))
		binary.Write(buf, binary.BigEndian, uint32(len(tps)))
		for _, tp := range tps {
			binary.Write(buf, binary.BigEndian, uint16(0)) // TODO: Write the error code instead
			binary.Write(buf, binary.BigEndian, uint32(tp.partition))
			binary.Write(buf, binary.BigEndian, uint32(tp.brokerId))
			binary.Write(buf, binary.BigEndian, uint32(0)) // replica set
			binary.Write(buf, binary.BigEndian, uint32(0)) // ISR set
		}
	}

	// Sample response:
	/*
		0x00, 0x00, 0x00, 0x02, // 0:3 number of brokers

		0x00, 0x00, 0x00, 0x01, // 4:7 broker ID
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't', // 8:18 hostname
		0xFF, 0xFF, 0xFF, 0xFF, // 19:22 port will be written here.

		0x00, 0x00, 0x00, 0x02, // 23:26 broker ID
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't', // 27:37 hostname
		0xFF, 0xFF, 0xFF, 0xFF, // 38:41 port will be written here.

		0x00, 0x00, 0x00, 0x03, // number of topic metadata records

		0x00, 0x00, // error: 0 means no error
		0x00, 0x07, 't', 'o', 'p', 'i', 'c', '_', 'a', // topic name
		0x00, 0x00, 0x00, 0x01, // number of partition metadata records for this topic
		0x00, 0x00, // error: 0 means no error
		0x00, 0x00, 0x00, 0x00, // partition ID
		0x00, 0x00, 0x00, 0x01, // broker ID of leader
		0x00, 0x00, 0x00, 0x00, // replica set
		0x00, 0x00, 0x00, 0x00, // ISR set

		0x00, 0x00, // error: 0 means no error
		0x00, 0x07, 't', 'o', 'p', 'i', 'c', '_', 'b', // topic name
		0x00, 0x00, 0x00, 0x01, // number of partition metadata records for this topic
		0x00, 0x00, // error: 0 means no error
		0x00, 0x00, 0x00, 0x00, // partition ID
		0x00, 0x00, 0x00, 0x02, // broker ID of leader
		0x00, 0x00, 0x00, 0x00, // replica set
		0x00, 0x00, 0x00, 0x00, // ISR set

		0x00, 0x00, // error: 0 means no error
		0x00, 0x07, 't', 'o', 'p', 'i', 'c', '_', 'c', // topic name
		0x00, 0x00, 0x00, 0x01, // number of partition metadata records for this topic
		0x00, 0x00, // error: 0 means no error
		0x00, 0x00, 0x00, 0x00, // partition ID
		0x00, 0x00, 0x00, 0x02, // broker ID of leader
		0x00, 0x00, 0x00, 0x00, // replica set
		0x00, 0x00, 0x00, 0x00, // ISR set
	*/

	return buf.Bytes()
}

func (b *MockBroker) ExpectMetadataRequest() *MetadataRequestExpectation {
	e := &MetadataRequestExpectation{}
	b.expectations <- e
	return e
}

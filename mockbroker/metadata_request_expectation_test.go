package mockbroker

import (
	"bytes"
	"testing"
)

type mockMockBroker struct {
	id   int
	port int32
}

func (b mockMockBroker) BrokerID() int { return b.id }
func (b mockMockBroker) Port() int32   { return b.port }

func TestMetadataRequestSerialization(t *testing.T) {

	exp := new(MetadataRequestExpectation).
		AddBroker(mockMockBroker{1, 8080}).
		AddBroker(mockMockBroker{2, 8081}).
		AddTopicPartition("topic_a", 0, 1).
		AddTopicPartition("topic_b", 0, 2).
		AddTopicPartition("topic_c", 0, 2)

	expected := []byte{
		0x00, 0x00, 0x00, 0x02, // 0:3 number of brokers

		0x00, 0x00, 0x00, 0x01, // 4:7 broker ID
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't', // 8:18 hostname
		0x00, 0x00, 0x1F, 0x90, // 19:22 port

		0x00, 0x00, 0x00, 0x02, // 23:26 broker ID
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't', // 27:37 hostname
		0x00, 0x00, 0x1F, 0x91, // 38:41 port

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
	}

	actual := exp.ResponseBytes()
	if bytes.Compare(actual, expected) != 0 {
		t.Errorf("\nExpected\n% 2x\nbut got\n% 2x", expected, actual)
	}
}

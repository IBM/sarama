package mockbroker

import (
	"bytes"
	"testing"
)

func TestProduceRequestSerialization(t *testing.T) {

	exp := new(ProduceRequestExpectation).
		AddTopicPartition("topic_b", 0, 1, nil).
		AddTopicPartition("topic_c", 0, 1, nil)

	expected := []byte{
		0x00, 0x00, 0x00, 0x02, // 0:3 number of topics

		0x00, 0x07, 't', 'o', 'p', 'i', 'c', '_', 'b', // 4:12 topic name
		0x00, 0x00, 0x00, 0x01, // 13:16 number of blocks for this topic
		0x00, 0x00, 0x00, 0x00, // 17:20 partition id
		0x00, 0x00, // 21:22 error: 0 means no error
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 23:30 offset

		0x00, 0x07, 't', 'o', 'p', 'i', 'c', '_', 'c', // topic name
		0x00, 0x00, 0x00, 0x01, // number of blocks for this topic
		0x00, 0x00, 0x00, 0x00, // partition id
		0x00, 0x00, // error: 0 means no error
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset
	}

	actual := exp.ResponseBytes()
	if bytes.Compare(actual, expected) != 0 {
		t.Error("\nExpected\n", expected, "\nbut got\n", actual)
	}
}

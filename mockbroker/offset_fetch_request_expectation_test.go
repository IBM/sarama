package mockbroker

import (
	"bytes"
	"testing"
)

func TestOffsetFetchRequestSerialization(t *testing.T) {

	exp := new(OffsetFetchRequestExpectation).
		AddTopicPartition("my_topic", 0, 0x010101)

	expected := []byte{
		0x00, 0x00, 0x00, 0x01, // number of topics
		0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c', // topic name
		0x00, 0x00, 0x00, 0x01, // number of blocks for this partition
		0x00, 0x00, 0x00, 0x00, // partition id
		0x00, 0x00, // error
		0x00, 0x00, 0x00, 0x01, // number of offsets
		0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x01, // offset
	}

	actual := exp.ResponseBytes()
	if bytes.Compare(actual, expected) != 0 {
		t.Error("\nExpected\n", expected, "\nbut got\n", actual)
	}
}

//go:build !functional

package sarama

import (
	"bytes"
	"reflect"
	"testing"
)

var (
	groupMemberMetadataV0 = []byte{
		0, 0, // Version
		0, 0, 0, 2, // Topic array length
		0, 3, 'o', 'n', 'e', // Topic one
		0, 3, 't', 'w', 'o', // Topic two
		0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
	}
	groupMemberAssignmentV0 = []byte{
		0, 0, // Version
		0, 0, 0, 1, // Topic array length
		0, 3, 'o', 'n', 'e', // Topic one
		0, 0, 0, 3, // Topic one, partition array length
		0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 4, // 0, 2, 4
		0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
	}

	// notably it looks like the old 3rdparty bsm/sarama-cluster incorrectly
	// set V1 in the member metadata when it sent the JoinGroup request so
	// we need to cope with that one being too short
	groupMemberMetadataV1Bad = []byte{
		0, 1, // Version
		0, 0, 0, 2, // Topic array length
		0, 3, 'o', 'n', 'e', // Topic one
		0, 3, 't', 'w', 'o', // Topic two
		0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
	}

	groupMemberMetadataV1 = []byte{
		0, 1, // Version
		0, 0, 0, 2, // Topic array length
		0, 3, 'o', 'n', 'e', // Topic one
		0, 3, 't', 'w', 'o', // Topic two
		0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
		0, 0, 0, 0, // OwnedPartitions KIP-429
	}

	groupMemberMetadataV3NilOwned = []byte{
		0, 3, // Version
		0, 0, 0, 1, // Topic array length
		0, 3, 'o', 'n', 'e', // Topic one
		0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
		0, 0, 0, 0, // OwnedPartitions KIP-429
		0, 0, 0, 64, // GenerationID
		0, 4, 'r', 'a', 'c', 'k', // RackID
	}
)

func TestConsumerGroupMemberMetadata(t *testing.T) {
	meta := &ConsumerGroupMemberMetadata{
		Version:  0,
		Topics:   []string{"one", "two"},
		UserData: []byte{0x01, 0x02, 0x03},
	}

	buf, err := encode(meta, nil)
	if err != nil {
		t.Error("Failed to encode data", err)
	} else if !bytes.Equal(groupMemberMetadataV0, buf) {
		t.Errorf("Encoded data does not match expectation\nexpected: %v\nactual: %v", groupMemberMetadataV0, buf)
	}

	meta2 := new(ConsumerGroupMemberMetadata)
	err = decode(buf, meta2, nil)
	if err != nil {
		t.Error("Failed to decode data", err)
	} else if !reflect.DeepEqual(meta, meta2) {
		t.Errorf("Encoded data does not match expectation\nexpected: %v\nactual: %v", meta, meta2)
	}
}

func TestConsumerGroupMemberMetadataV1Decode(t *testing.T) {
	meta := new(ConsumerGroupMemberMetadata)
	if err := decode(groupMemberMetadataV1, meta, nil); err != nil {
		t.Error("Failed to decode V1 data", err)
	}
	if err := decode(groupMemberMetadataV1Bad, meta, nil); err != nil {
		t.Error("Failed to decode V1 'bad' data", err)
	}
}

func TestConsumerGroupMemberMetadataV3Decode(t *testing.T) {
	meta := new(ConsumerGroupMemberMetadata)
	if err := decode(groupMemberMetadataV3NilOwned, meta, nil); err != nil {
		t.Error("Failed to decode V3 data", err)
	}
}

func TestConsumerGroupMemberAssignment(t *testing.T) {
	amt := &ConsumerGroupMemberAssignment{
		Version: 0,
		Topics: map[string][]int32{
			"one": {0, 2, 4},
		},
		UserData: []byte{0x01, 0x02, 0x03},
	}

	buf, err := encode(amt, nil)
	if err != nil {
		t.Error("Failed to encode data", err)
	} else if !bytes.Equal(groupMemberAssignmentV0, buf) {
		t.Errorf("Encoded data does not match expectation\nexpected: %v\nactual: %v", groupMemberAssignmentV0, buf)
	}

	amt2 := new(ConsumerGroupMemberAssignment)
	err = decode(buf, amt2, nil)
	if err != nil {
		t.Error("Failed to decode data", err)
	} else if !reflect.DeepEqual(amt, amt2) {
		t.Errorf("Encoded data does not match expectation\nexpected: %v\nactual: %v", amt, amt2)
	}
}

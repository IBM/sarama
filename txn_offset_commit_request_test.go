//go:build !functional

package sarama

import "testing"

var (
	txnOffsetCommitRequest = []byte{
		0, 3, 't', 'x', 'n',
		0, 7, 'g', 'r', 'o', 'u', 'p', 'i', 'd',
		0, 0, 0, 0, 0, 0, 31, 64, // producer ID
		0, 1, // producer epoch
		0, 0, 0, 1, // 1 topic
		0, 5, 't', 'o', 'p', 'i', 'c',
		0, 0, 0, 1, // 1 partition
		0, 0, 0, 2, // partition no 2
		0, 0, 0, 0, 0, 0, 0, 123,
		255, 255, // no meta data
	}

	txnOffsetCommitRequestV2 = []byte{
		0, 3, 't', 'x', 'n',
		0, 7, 'g', 'r', 'o', 'u', 'p', 'i', 'd',
		0, 0, 0, 0, 0, 0, 31, 64, // producer ID
		0, 1, // producer epoch
		0, 0, 0, 1, // 1 topic
		0, 5, 't', 'o', 'p', 'i', 'c',
		0, 0, 0, 1, // 1 partition
		0, 0, 0, 2, // partition no 2
		0, 0, 0, 0, 0, 0, 0, 123,
		0, 0, 0, 9, // leader epoch
		255, 255, // no meta data
	}

	txnOffsetCommitRequestV3 = []byte{
		4, 't', 'x', 'n', // transactional ID (compact string)
		8, 'g', 'r', 'o', 'u', 'p', 'i', 'd', // group ID (compact string)
		0, 0, 0, 0, 0, 0, 31, 64, // producer ID
		0, 1, // producer epoch
		0, 0, 0, 5, // generation ID
		4, 'm', 'i', 'd', // member ID (compact string)
		4, 'g', 'i', 'd', // group instance ID (compact nullable string)
		2,                          // 1 topic (compact array)
		6, 't', 'o', 'p', 'i', 'c', // topic name (compact string)
		2,          // 1 partition (compact array)
		0, 0, 0, 2, // partition no 2
		0, 0, 0, 0, 0, 0, 0, 123, // committed offset
		0, 0, 0, 9, // leader epoch
		0, // no meta data (compact nullable string, null)
		0, // partition tagged fields
		0, // topic tagged fields
		0, // request tagged fields
	}
)

func TestTxnOffsetCommitRequest(t *testing.T) {
	req := &TxnOffsetCommitRequest{
		TransactionalID: "txn",
		GroupID:         "groupid",
		ProducerID:      8000,
		ProducerEpoch:   1,
		Topics: map[string][]*PartitionOffsetMetadata{
			"topic": {{
				Offset:    123,
				Partition: 2,
			}},
		},
	}

	testRequest(t, "V0", req, txnOffsetCommitRequest)
}

func TestTxnOffsetCommitRequestV2(t *testing.T) {
	req := &TxnOffsetCommitRequest{
		Version:         2,
		TransactionalID: "txn",
		GroupID:         "groupid",
		ProducerID:      8000,
		ProducerEpoch:   1,
		Topics: map[string][]*PartitionOffsetMetadata{
			"topic": {{
				Offset:      123,
				Partition:   2,
				LeaderEpoch: 9,
			}},
		},
	}

	testRequest(t, "V2", req, txnOffsetCommitRequestV2)
}

func TestTxnOffsetCommitRequestV3(t *testing.T) {
	groupInstanceID := "gid"
	req := &TxnOffsetCommitRequest{
		Version:         3,
		TransactionalID: "txn",
		GroupID:         "groupid",
		ProducerID:      8000,
		ProducerEpoch:   1,
		GenerationID:    5,
		MemberID:        "mid",
		GroupInstanceID: &groupInstanceID,
		Topics: map[string][]*PartitionOffsetMetadata{
			"topic": {{
				Offset:      123,
				Partition:   2,
				LeaderEpoch: 9,
			}},
		},
	}

	testRequest(t, "V3", req, txnOffsetCommitRequestV3)
}

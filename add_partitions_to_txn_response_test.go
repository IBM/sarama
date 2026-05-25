//go:build !functional

package sarama

import (
	"reflect"
	"testing"
	"time"

	assert "github.com/stretchr/testify/require"
)

var addPartitionsToTxnResponse = []byte{
	0, 0, 0, 100,
	0, 0, 0, 1,
	0, 5, 't', 'o', 'p', 'i', 'c',
	0, 0, 0, 1, // 1 partition error
	0, 0, 0, 2, // partition 2
	0, 48, // error
}

func TestAddPartitionsToTxnResponse(t *testing.T) {
	resp := &AddPartitionsToTxnResponse{
		ThrottleTime: 100 * time.Millisecond,
		Errors: map[string][]*PartitionError{
			"topic": {{
				Err:       ErrInvalidTxnState,
				Partition: 2,
			}},
		},
	}

	testResponse(t, "", resp, addPartitionsToTxnResponse)
}

func TestAddPartitionsToTxnResponseV3(t *testing.T) {
	resp := &AddPartitionsToTxnResponse{
		Version:      3,
		ThrottleTime: 100 * time.Millisecond,
		Errors: map[string][]*PartitionError{
			"topic": {{
				Err:       ErrInvalidTxnState,
				Partition: 2,
			}},
		},
	}

	testResponseWithoutByteComparison(t, "v3", resp)
}

func TestAddPartitionsToTxnResponseV4Batched(t *testing.T) {
	resp := &AddPartitionsToTxnResponse{
		Version:      4,
		ThrottleTime: 100 * time.Millisecond,
		ResultsByTransaction: []AddPartitionsToTxnTransactionResult{
			{
				TransactionalID: "txn",
				Errors: map[string][]*PartitionError{
					"topic": {{Err: ErrInvalidTxnState, Partition: 2}},
				},
			},
		},
	}

	buf, err := encode(resp, nil)
	assert.NoError(t, err)
	decoded := new(AddPartitionsToTxnResponse)
	decoded.Version = 4
	assert.NoError(t, versionedDecode(buf, decoded, 4, nil))
	assert.Equal(t, resp.ResultsByTransaction, decoded.ResultsByTransaction)
	assert.Equal(t, resp.ResultsByTransaction[0].Errors, decoded.Errors)
}

func testResponseWithoutByteComparison(t *testing.T, name string, res protocolBody) {
	t.Helper()
	encoded, err := encode(res, nil)
	if err != nil {
		t.Fatalf("encoding %s failed: %v", name, err)
	}
	decoded := reflect.New(reflect.TypeOf(res).Elem()).Interface().(versionedDecoder)
	if err := versionedDecode(encoded, decoded, res.version(), nil); err != nil {
		t.Fatalf("decoding %s failed: %v", name, err)
	}
}

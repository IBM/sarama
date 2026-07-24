//go:build !functional

package sarama

import (
	"errors"
	"testing"
)

var listTransactionsRequestV0 = []byte{
	2,                                    // StateFilters (one element array)
	8, 'O', 'n', 'g', 'o', 'i', 'n', 'g', // state filter
	2,                        // ProducerIdFilters (one element array)
	0, 0, 0, 0, 0, 0, 3, 232, // producer id filter
	0, // empty tagged fields
}

var listTransactionsRequestV1 = []byte{
	2,                                    // StateFilters (one element array)
	8, 'O', 'n', 'g', 'o', 'i', 'n', 'g', // state filter
	2,                        // ProducerIdFilters (one element array)
	0, 0, 0, 0, 0, 0, 3, 232, // producer id filter
	0, 0, 0, 0, 0, 0, 0, 100, // duration filter ms
	0, // empty tagged fields
}

func TestListTransactionsRequest(t *testing.T) {
	request := &ListTransactionsRequest{
		Version:           0,
		StateFilters:      []string{TransactionStateOngoing},
		ProducerIDFilters: []int64{1000},
		DurationFilter:    -1,
	}

	testRequest(t, "v0", request, listTransactionsRequestV0)

	request.Version = 1
	request.DurationFilter = 100
	testRequest(t, "v1", request, listTransactionsRequestV1)
}

// A DurationFilter can only be encoded from v1. If a v0 request still carries an
// explicit filter (e.g. restrictApiVersion downgraded the version after the
// admin call built it), encode must fail rather than silently drop the filter.
func TestListTransactionsRequestDurationFilterRequiresV1(t *testing.T) {
	request := &ListTransactionsRequest{
		Version:        0,
		DurationFilter: 5000,
	}
	err := request.encode(&prepEncoder{})
	target := PacketEncodingError{}
	if !errors.As(err, &target) {
		t.Fatalf("expected PacketEncodingError, got %v", err)
	}

	// A disabled (negative) filter is always safe, even at v0.
	request.DurationFilter = -1
	if err := request.encode(&prepEncoder{}); err != nil {
		t.Fatalf("disabled filter must encode at v0, got %v", err)
	}
}

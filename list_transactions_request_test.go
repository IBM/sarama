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

// NewListTransactionsRequest must default DurationFilter to -1 (no filter) so a
// caller that sets no filter can be encoded at any version, and must pick the
// request version from the configured Kafka version.
func TestNewListTransactionsRequest(t *testing.T) {
	pre := NewListTransactionsRequest(V3_7_0_0)
	if pre.Version != 0 {
		t.Fatalf("expected v0 below V3_8_0_0, got v%d", pre.Version)
	}
	if pre.DurationFilter != -1 {
		t.Fatalf("expected DurationFilter -1, got %d", pre.DurationFilter)
	}
	// The default (no filter) must encode even at v0.
	if err := pre.encode(&prepEncoder{}); err != nil {
		t.Fatalf("default request must encode at v0, got %v", err)
	}

	post := NewListTransactionsRequest(V3_8_0_0)
	if post.Version != 1 {
		t.Fatalf("expected v1 at V3_8_0_0, got v%d", post.Version)
	}
	if post.DurationFilter != -1 {
		t.Fatalf("expected DurationFilter -1, got %d", post.DurationFilter)
	}
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

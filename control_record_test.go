//go:build !functional

package sarama

import (
	"testing"
)

var (
	abortTxCtrlRecKey = []byte{
		0, 0, // version
		0, 0, // TX_ABORT = 0
	}
	abortTxCtrlRecValue = []byte{
		0, 0, // version
		0, 0, 0, 10, // coordinator epoch
	}
	commitTxCtrlRecKey = []byte{
		0, 0, // version
		0, 1, // TX_COMMIT = 1
	}
	commitTxCtrlRecValue = []byte{
		0, 0, // version
		0, 0, 0, 15, // coordinator epoch
	}
	unknownCtrlRecKey = []byte{
		0, 0, // version
		0, 128, // UNKNOWN = -1
	}
	// empty value for unknown record
	unknownCtrlRecValue = []byte{}
)

func testDecode(t *testing.T, tp string, key []byte, value []byte) ControlRecord {
	controlRecord := ControlRecord{}
	err := controlRecord.decode(&realDecoder{raw: key}, &realDecoder{raw: value})
	if err != nil {
		t.Error("Decoding control record of type " + tp + " failed")
		return ControlRecord{}
	}
	return controlRecord
}

func assertRecordType(t *testing.T, r *ControlRecord, expected ControlRecordType) {
	if r.Type != expected {
		t.Errorf("control record type mismatch, expected: %v, have %v", expected, r.Type)
	}
}

func TestDecodingControlRecords(t *testing.T) {
	abortTx := testDecode(t, "abort transaction", abortTxCtrlRecKey, abortTxCtrlRecValue)

	assertRecordType(t, &abortTx, ControlRecordAbort)

	if abortTx.CoordinatorEpoch != 10 {
		t.Errorf("abort tx control record coordinator epoch mismatch")
	}

	commitTx := testDecode(t, "commit transaction", commitTxCtrlRecKey, commitTxCtrlRecValue)

	if commitTx.CoordinatorEpoch != 15 {
		t.Errorf("commit tx control record coordinator epoch mismatch")
	}
	assertRecordType(t, &commitTx, ControlRecordCommit)

	unknown := testDecode(t, "unknown", unknownCtrlRecKey, unknownCtrlRecValue)

	assertRecordType(t, &unknown, ControlRecordUnknown)
}

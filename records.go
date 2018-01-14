package sarama

import (
	"fmt"
)

const (
	unknownRecords = iota
	legacyRecords
	defaultRecords

	magicOffset = 16
	magicLength = 1
)

// Records implements a union type containing either a RecordBatch or a legacy MessageSet.
type Records struct {
	recordsType    int
	msgSet         *MessageSet
	recordBatchSet *RecordBatchSet
}

func newLegacyRecords(msgSet *MessageSet) Records {
	return Records{recordsType: legacyRecords, msgSet: msgSet}
}

func newDefaultRecords(batches []*RecordBatch) Records {
	return Records{recordsType: defaultRecords, recordBatchSet: &RecordBatchSet{batches}}
}

// setTypeFromFields sets type of Records depending on which of msgSet or recordBatch is not nil.
// The first return value indicates whether both fields are nil (and the type is not set).
// If both fields are not nil, it returns an error.
func (r *Records) setTypeFromFields() (bool, error) {
	if r.msgSet == nil && r.recordBatchSet == nil {
		return true, nil
	}
	if r.msgSet != nil && r.recordBatchSet != nil {
		return false, fmt.Errorf("both msgSet and recordBatchSet are set, but record type is unknown")
	}
	r.recordsType = defaultRecords
	if r.msgSet != nil {
		r.recordsType = legacyRecords
	}
	return false, nil
}

func (r *Records) encode(pe packetEncoder) error {
	if r.recordsType == unknownRecords {
		if empty, err := r.setTypeFromFields(); err != nil || empty {
			return err
		}
	}

	switch r.recordsType {
	case legacyRecords:
		if r.msgSet == nil {
			return nil
		}
		return r.msgSet.encode(pe)
	case defaultRecords:
		if r.recordBatchSet == nil {
			return nil
		}
		return r.recordBatchSet.encode(pe)
	}
	return fmt.Errorf("unknown records type: %v", r.recordsType)
}

func (r *Records) setTypeFromMagic(pd packetDecoder) error {
	dec, err := pd.peek(magicOffset, magicLength)
	if err != nil {
		return err
	}

	magic, err := dec.getInt8()
	if err != nil {
		return err
	}

	r.recordsType = defaultRecords
	if magic < 2 {
		r.recordsType = legacyRecords
	}
	return nil
}

func (r *Records) decode(pd packetDecoder) error {
	if r.recordsType == unknownRecords {
		if err := r.setTypeFromMagic(pd); err != nil {
			return nil
		}
	}

	switch r.recordsType {
	case legacyRecords:
		r.msgSet = &MessageSet{}
		return r.msgSet.decode(pd)
	case defaultRecords:
		r.recordBatchSet = &RecordBatchSet{batches: []*RecordBatch{}}
		return r.recordBatchSet.decode(pd)
	}
	return fmt.Errorf("unknown records type: %v", r.recordsType)
}

func (r *Records) numRecords() (int, error) {
	if r.recordsType == unknownRecords {
		if empty, err := r.setTypeFromFields(); err != nil || empty {
			return 0, err
		}
	}

	switch r.recordsType {
	case legacyRecords:
		if r.msgSet == nil {
			return 0, nil
		}
		return len(r.msgSet.Messages), nil
	case defaultRecords:
		if r.recordBatchSet == nil {
			return 0, nil
		}
		s := 0
		for i := range r.recordBatchSet.batches {
			s += len(r.recordBatchSet.batches[i].Records)
		}
		return s, nil
	}
	return 0, fmt.Errorf("unknown records type: %v", r.recordsType)
}

func (r *Records) isPartial() (bool, error) {
	if r.recordsType == unknownRecords {
		if empty, err := r.setTypeFromFields(); err != nil || empty {
			return false, err
		}
	}

	switch r.recordsType {
	case unknownRecords:
		return false, nil
	case legacyRecords:
		if r.msgSet == nil {
			return false, nil
		}
		return r.msgSet.PartialTrailingMessage, nil
	case defaultRecords:
		if r.recordBatchSet == nil {
			return false, nil
		}
		if len(r.recordBatchSet.batches) == 1 {
			return r.recordBatchSet.batches[0].PartialTrailingRecord, nil
		}
		return false, nil
	}
	return false, fmt.Errorf("unknown records type: %v", r.recordsType)
}

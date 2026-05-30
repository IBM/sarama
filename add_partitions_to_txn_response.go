package sarama

import (
	"time"
)

type AddPartitionsToTxnTransactionResult struct {
	TransactionalID string
	Errors          map[string][]*PartitionError
}

// AddPartitionsToTxnResponse is a partition errors to transaction type
type AddPartitionsToTxnResponse struct {
	Version              int16
	ThrottleTime         time.Duration
	Errors               map[string][]*PartitionError
	ErrorCode            KError                                // v4
	ResultsByTransaction []AddPartitionsToTxnTransactionResult // v4
}

func (a *AddPartitionsToTxnResponse) setVersion(v int16) {
	a.Version = v
}

func (a *AddPartitionsToTxnResponse) encode(pe packetEncoder) error {
	pe.putDurationMs(a.ThrottleTime)

	if a.Version >= 4 {
		pe.putKError(a.ErrorCode)
		results := a.ResultsByTransaction
		if results == nil {
			results = []AddPartitionsToTxnTransactionResult{{Errors: a.Errors}}
		}
		if err := pe.putArrayLength(len(results)); err != nil {
			return err
		}
		for i := range results {
			if err := encodeAddPartitionsToTxnTransactionResult(pe, &results[i]); err != nil {
				return err
			}
		}
		pe.putEmptyTaggedFieldArray()
		return nil
	}

	if err := pe.putArrayLength(len(a.Errors)); err != nil {
		return err
	}

	for topic, e := range a.Errors {
		if err := pe.putString(topic); err != nil {
			return err
		}
		if err := pe.putArrayLength(len(e)); err != nil {
			return err
		}
		for _, partitionError := range e {
			if err := partitionError.encode(pe); err != nil {
				return err
			}
		}
		if a.isFlexible() {
			pe.putEmptyTaggedFieldArray()
		}
	}

	if a.isFlexible() {
		pe.putEmptyTaggedFieldArray()
	}
	return nil
}

func encodeAddPartitionsToTxnTransactionResult(pe packetEncoder, r *AddPartitionsToTxnTransactionResult) error {
	if err := pe.putString(r.TransactionalID); err != nil {
		return err
	}
	if err := pe.putArrayLength(len(r.Errors)); err != nil {
		return err
	}
	for topic, partitions := range r.Errors {
		if err := pe.putString(topic); err != nil {
			return err
		}
		if err := pe.putArrayLength(len(partitions)); err != nil {
			return err
		}
		for _, p := range partitions {
			pe.putInt32(p.Partition)
			pe.putKError(p.Err)
			pe.putEmptyTaggedFieldArray()
		}
		pe.putEmptyTaggedFieldArray()
	}
	pe.putEmptyTaggedFieldArray()
	return nil
}

func (a *AddPartitionsToTxnResponse) decode(pd packetDecoder, version int16) (err error) {
	a.Version = version
	if a.ThrottleTime, err = pd.getDurationMs(); err != nil {
		return err
	}

	if a.Version >= 4 {
		if a.ErrorCode, err = pd.getKError(); err != nil {
			return err
		}
		n, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		a.ResultsByTransaction = make([]AddPartitionsToTxnTransactionResult, n)
		for i := 0; i < n; i++ {
			if err := decodeAddPartitionsToTxnTransactionResult(pd, &a.ResultsByTransaction[i]); err != nil {
				return err
			}
		}
		if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
		if len(a.ResultsByTransaction) > 0 {
			a.Errors = a.ResultsByTransaction[0].Errors
		}
		return nil
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	a.Errors = make(map[string][]*PartitionError, n)

	for i := 0; i < n; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}

		m, err := pd.getArrayLength()
		if err != nil {
			return err
		}

		a.Errors[topic] = make([]*PartitionError, m)

		for j := 0; j < m; j++ {
			a.Errors[topic][j] = new(PartitionError)
			if err := a.Errors[topic][j].decode(pd, version); err != nil {
				return err
			}
		}
		if a.isFlexible() {
			if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
				return err
			}
		}
	}

	if a.isFlexible() {
		if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}
	return nil
}

func decodeAddPartitionsToTxnTransactionResult(pd packetDecoder, r *AddPartitionsToTxnTransactionResult) (err error) {
	if r.TransactionalID, err = pd.getString(); err != nil {
		return err
	}
	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	r.Errors = make(map[string][]*PartitionError, n)
	for i := 0; i < n; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		m, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		r.Errors[topic] = make([]*PartitionError, m)
		for j := 0; j < m; j++ {
			pe := new(PartitionError)
			if pe.Partition, err = pd.getInt32(); err != nil {
				return err
			}
			if pe.Err, err = pd.getKError(); err != nil {
				return err
			}
			if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
				return err
			}
			r.Errors[topic][j] = pe
		}
		if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}
	_, err = pd.getEmptyTaggedFieldArray()
	return err
}

func (a *AddPartitionsToTxnResponse) key() int16 {
	return apiKeyAddPartitionsToTxn
}

func (a *AddPartitionsToTxnResponse) version() int16 {
	return a.Version
}

func (a *AddPartitionsToTxnResponse) headerVersion() int16 {
	if a.isFlexible() {
		return 1
	}
	return 0
}

func (a *AddPartitionsToTxnResponse) isValidVersion() bool {
	return a.Version >= 0 && a.Version <= 5
}

func (a *AddPartitionsToTxnResponse) isFlexible() bool {
	return a.isFlexibleVersion(a.Version)
}

func (a *AddPartitionsToTxnResponse) isFlexibleVersion(version int16) bool {
	return version >= 3
}

func (a *AddPartitionsToTxnResponse) requiredVersion() KafkaVersion {
	switch a.Version {
	case 5:
		return V3_8_0_0
	case 4:
		return V3_6_0_0
	case 3:
		return V2_8_0_0
	case 2:
		return V2_7_0_0
	case 1:
		return V2_0_0_0
	default:
		return V0_11_0_0
	}
}

func (r *AddPartitionsToTxnResponse) throttleTime() time.Duration {
	return r.ThrottleTime
}

// PartitionError is a partition error type
type PartitionError struct {
	Partition int32
	Err       KError
}

func (p *PartitionError) encode(pe packetEncoder) error {
	pe.putInt32(p.Partition)
	pe.putKError(p.Err)
	return nil
}

func (p *PartitionError) decode(pd packetDecoder, version int16) (err error) {
	if p.Partition, err = pd.getInt32(); err != nil {
		return err
	}

	p.Err, err = pd.getKError()
	if err != nil {
		return err
	}

	return nil
}

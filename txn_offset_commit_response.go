package sarama

import (
	"time"
)

type TxnOffsetCommitResponse struct {
	Version      int16
	ThrottleTime time.Duration
	Topics       map[string][]*PartitionError
}

func (t *TxnOffsetCommitResponse) setVersion(v int16) {
	t.Version = v
}

func (t *TxnOffsetCommitResponse) encode(pe packetEncoder) error {
	pe.putDurationMs(t.ThrottleTime)
	if err := pe.putArrayLength(len(t.Topics)); err != nil {
		return err
	}

	for topic, e := range t.Topics {
		if err := pe.putString(topic); err != nil {
			return err
		}
		if err := pe.putArrayLength(len(e)); err != nil {
			return err
		}
		for _, partitionError := range e {
			pe.putInt32(partitionError.Partition)
			pe.putKError(partitionError.Err)
			if t.isFlexible() {
				pe.putEmptyTaggedFieldArray()
			}
		}
		if t.isFlexible() {
			pe.putEmptyTaggedFieldArray()
		}
	}

	if t.isFlexible() {
		pe.putEmptyTaggedFieldArray()
	}
	return nil
}

func (t *TxnOffsetCommitResponse) decode(pd packetDecoder, version int16) (err error) {
	t.Version = version
	if t.ThrottleTime, err = pd.getDurationMs(); err != nil {
		return err
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	t.Topics = make(map[string][]*PartitionError, n)

	for i := 0; i < n; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}

		m, err := pd.getArrayLength()
		if err != nil {
			return err
		}

		t.Topics[topic] = make([]*PartitionError, m)

		for j := 0; j < m; j++ {
			pe := new(PartitionError)
			if pe.Partition, err = pd.getInt32(); err != nil {
				return err
			}
			if pe.Err, err = pd.getKError(); err != nil {
				return err
			}
			if t.isFlexible() {
				if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
					return err
				}
			}
			t.Topics[topic][j] = pe
		}
		if t.isFlexible() {
			if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
				return err
			}
		}
	}

	if t.isFlexible() {
		if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}

	return nil
}

func (a *TxnOffsetCommitResponse) key() int16 {
	return apiKeyTxnOffsetCommit
}

func (a *TxnOffsetCommitResponse) version() int16 {
	return a.Version
}

func (a *TxnOffsetCommitResponse) headerVersion() int16 {
	if a.isFlexible() {
		return 1
	}
	return 0
}

func (a *TxnOffsetCommitResponse) isValidVersion() bool {
	return a.Version >= 0 && a.Version <= 5
}

func (a *TxnOffsetCommitResponse) isFlexible() bool {
	return a.isFlexibleVersion(a.Version)
}

func (a *TxnOffsetCommitResponse) isFlexibleVersion(version int16) bool {
	return version >= 3
}

func (a *TxnOffsetCommitResponse) requiredVersion() KafkaVersion {
	switch a.Version {
	case 5:
		return V4_0_0_0
	case 4:
		return V3_5_0_0
	case 3:
		return V3_0_0_0
	case 2:
		return V2_1_0_0
	case 1:
		return V2_0_0_0
	case 0:
		return V0_11_0_0
	default:
		return V2_1_0_0
	}
}

func (r *TxnOffsetCommitResponse) throttleTime() time.Duration {
	return r.ThrottleTime
}

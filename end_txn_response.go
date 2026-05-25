package sarama

import (
	"time"
)

type EndTxnResponse struct {
	Version       int16
	ThrottleTime  time.Duration
	Err           KError
	ProducerID    int64 // v5
	ProducerEpoch int16 // v5
}

func (e *EndTxnResponse) setVersion(v int16) {
	e.Version = v
}

func (e *EndTxnResponse) encode(pe packetEncoder) error {
	pe.putDurationMs(e.ThrottleTime)
	pe.putKError(e.Err)
	if e.Version >= 5 {
		pe.putInt64(e.ProducerID)
		pe.putInt16(e.ProducerEpoch)
	}
	if e.isFlexible() {
		pe.putEmptyTaggedFieldArray()
	}
	return nil
}

func (e *EndTxnResponse) decode(pd packetDecoder, version int16) (err error) {
	e.Version = version
	e.ProducerID = -1
	e.ProducerEpoch = -1

	if e.ThrottleTime, err = pd.getDurationMs(); err != nil {
		return err
	}

	e.Err, err = pd.getKError()
	if err != nil {
		return err
	}

	if e.Version >= 5 {
		if e.ProducerID, err = pd.getInt64(); err != nil {
			return err
		}
		if e.ProducerEpoch, err = pd.getInt16(); err != nil {
			return err
		}
	}

	if e.isFlexible() {
		if _, err = pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}

	return nil
}

func (e *EndTxnResponse) key() int16 {
	return apiKeyEndTxn
}

func (e *EndTxnResponse) version() int16 {
	return e.Version
}

func (r *EndTxnResponse) headerVersion() int16 {
	if r.isFlexible() {
		return 1
	}
	return 0
}

func (e *EndTxnResponse) isValidVersion() bool {
	return e.Version >= 0 && e.Version <= 5
}

func (e *EndTxnResponse) isFlexible() bool {
	return e.isFlexibleVersion(e.Version)
}

func (e *EndTxnResponse) isFlexibleVersion(version int16) bool {
	return version >= 3
}

func (e *EndTxnResponse) requiredVersion() KafkaVersion {
	switch e.Version {
	case 5:
		return V4_0_0_0
	case 4:
		return V3_5_0_0
	case 3:
		return V3_0_0_0
	case 2:
		return V2_7_0_0
	case 1:
		return V2_0_0_0
	default:
		return V0_11_0_0
	}
}

func (r *EndTxnResponse) throttleTime() time.Duration {
	return r.ThrottleTime
}

package sarama

import "time"

type ExpireDelegationTokenResponse struct {
	Version      int16
	ErrorCode    KError
	ExpiryTime   time.Time
	ThrottleTime time.Duration
}

func (e *ExpireDelegationTokenResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(e.ErrorCode))
	pe.putInt64(e.ExpiryTime.UnixMilli())
	pe.putInt32(int32(e.ThrottleTime / time.Millisecond))

	if e.Version > 1 {
		pe.putEmptyTaggedFieldArray()
	}

	return nil
}

func (e *ExpireDelegationTokenResponse) decode(pd packetDecoder, version int16) (err error) {
	e.Version = version

	if errCode, err := pd.getInt16(); err == nil {
		e.ErrorCode = KError(errCode)
	} else {
		return err
	}

	if ms, err := pd.getInt64(); err == nil {
		e.ExpiryTime = time.UnixMilli(ms)
	} else {
		return err
	}

	var throttle int32
	if throttle, err = pd.getInt32(); err == nil {
		e.ThrottleTime = time.Duration(throttle) * time.Millisecond
	}

	if e.Version > 1 && err == nil {
		_, err = pd.getEmptyTaggedFieldArray()
	}

	return err
}

func (e *ExpireDelegationTokenResponse) key() int16 {
	return 40
}

func (e *ExpireDelegationTokenResponse) version() int16 {
	return e.Version
}

func (e *ExpireDelegationTokenResponse) headerVersion() int16 {
	if e.Version > 1 {
		return 1
	}
	return 0
}

func (e *ExpireDelegationTokenResponse) isValidVersion() bool {
	return e.Version >= 0 && e.Version <= 2
}

func (e *ExpireDelegationTokenResponse) requiredVersion() KafkaVersion {
	return V1_1_0_0
}

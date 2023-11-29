package sarama

import "time"

type RenewDelegationTokenResponse struct {
	Version      int16
	ErrorCode    KError
	ExpiryTime   time.Time
	ThrottleTime time.Duration
}

func (r *RenewDelegationTokenResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(r.ErrorCode))
	pe.putInt64(r.ExpiryTime.UnixMilli())
	pe.putInt32(int32(r.ThrottleTime / time.Millisecond))

	if r.Version > 1 {
		pe.putEmptyTaggedFieldArray()
	}

	return nil
}

func (r *RenewDelegationTokenResponse) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version

	if errCode, err := pd.getInt16(); err == nil {
		r.ErrorCode = KError(errCode)
	} else {
		return err
	}

	if ms, err := pd.getInt64(); err == nil {
		r.ExpiryTime = time.UnixMilli(ms)
	} else {
		return err
	}

	var throttle int32
	if throttle, err = pd.getInt32(); err == nil {
		r.ThrottleTime = time.Duration(throttle) * time.Millisecond
	}

	if r.Version > 1 && err == nil {
		_, err = pd.getEmptyTaggedFieldArray()
	}

	return err
}

func (r *RenewDelegationTokenResponse) key() int16 {
	return 39
}

func (r *RenewDelegationTokenResponse) version() int16 {
	return r.Version
}

func (r *RenewDelegationTokenResponse) headerVersion() int16 {
	if r.Version > 1 {
		return 1
	}
	return 0
}

func (r *RenewDelegationTokenResponse) isValidVersion() bool {
	return r.Version >= 0 && r.Version <= 2
}

func (r *RenewDelegationTokenResponse) requiredVersion() KafkaVersion {
	return V1_1_0_0
}

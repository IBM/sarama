package sarama

import "time"

type RenewDelegationTokenRequest struct {
	Version       int16
	HMAC          []byte
	RenewalPeriod time.Duration
}

func (r *RenewDelegationTokenRequest) encode(pe packetEncoder) error {
	if r.Version > 1 {
		if err := pe.putCompactBytes(r.HMAC); err != nil {
			return err
		}
	} else {
		if err := pe.putBytes(r.HMAC); err != nil {
			return err
		}
	}

	pe.putInt64(r.RenewalPeriod.Milliseconds())

	if r.Version > 1 {
		pe.putEmptyTaggedFieldArray()
	}

	return nil
}

func (r *RenewDelegationTokenRequest) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version

	if version > 1 {
		if r.HMAC, err = pd.getCompactBytes(); err != nil {
			return err
		}
	} else {
		if r.HMAC, err = pd.getBytes(); err != nil {
			return err
		}
	}

	var ms int64
	if ms, err = pd.getInt64(); err == nil {
		r.RenewalPeriod = time.Duration(ms) * time.Millisecond
	}

	if version > 1 && err == nil {
		_, err = pd.getEmptyTaggedFieldArray()
	}
	return err
}

func (r *RenewDelegationTokenRequest) key() int16 {
	return 39
}

func (r *RenewDelegationTokenRequest) version() int16 {
	return r.Version
}

func (r *RenewDelegationTokenRequest) headerVersion() int16 {
	if r.Version > 1 {
		return 2
	}
	return 1
}

func (r *RenewDelegationTokenRequest) isValidVersion() bool {
	return r.Version >= 0 && r.Version <= 2
}

func (r *RenewDelegationTokenRequest) requiredVersion() KafkaVersion {
	return V1_1_0_0
}

package sarama

import "time"

type ExpireDelegationTokenRequest struct {
	Version      int16
	HMAC         []byte
	ExpiryPeriod time.Duration
}

func (e *ExpireDelegationTokenRequest) encode(pe packetEncoder) error {
	if e.Version > 1 {
		if err := pe.putCompactBytes(e.HMAC); err != nil {
			return err
		}
	} else {
		if err := pe.putBytes(e.HMAC); err != nil {
			return err
		}
	}

	pe.putInt64(e.ExpiryPeriod.Milliseconds())

	if e.Version > 1 {
		pe.putEmptyTaggedFieldArray()
	}

	return nil
}

func (e *ExpireDelegationTokenRequest) decode(pd packetDecoder, version int16) (err error) {
	e.Version = version

	if version > 1 {
		if e.HMAC, err = pd.getCompactBytes(); err != nil {
			return err
		}
	} else {
		if e.HMAC, err = pd.getBytes(); err != nil {
			return err
		}
	}

	var ms int64
	if ms, err = pd.getInt64(); err == nil {
		e.ExpiryPeriod = time.Duration(ms) * time.Millisecond
	}

	if version > 1 && err == nil {
		_, err = pd.getEmptyTaggedFieldArray()
	}
	return err
}

func (e *ExpireDelegationTokenRequest) key() int16 {
	return 40
}

func (e *ExpireDelegationTokenRequest) version() int16 {
	return e.Version
}

func (e *ExpireDelegationTokenRequest) headerVersion() int16 {
	if e.Version > 1 {
		return 2
	}
	return 1
}

func (e *ExpireDelegationTokenRequest) isValidVersion() bool {
	return e.Version >= 0 && e.Version <= 2
}

func (e *ExpireDelegationTokenRequest) requiredVersion() KafkaVersion {
	return V1_1_0_0
}

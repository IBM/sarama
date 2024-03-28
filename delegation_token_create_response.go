package sarama

import (
	"time"
)

type CreateDelegationTokenResponse struct {
	DelegationToken
	Version      int16
	ErrorCode    KError
	ThrottleTime time.Duration
}

func (c *CreateDelegationTokenResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(c.ErrorCode))

	if err := c.DelegationToken.encode(pe, c.Version); err != nil {
		return err
	}

	pe.putInt32(int32(c.ThrottleTime / time.Millisecond))

	if c.Version > 1 {
		pe.putEmptyTaggedFieldArray()
	}

	return nil
}

func (c *CreateDelegationTokenResponse) decode(pd packetDecoder, version int16) (err error) {
	c.Version = version

	if errCode, err := pd.getInt16(); err == nil {
		c.ErrorCode = KError(errCode)
	} else {
		return err
	}

	if err := c.DelegationToken.decode(pd, version); err != nil {
		return err
	}

	var throttle int32
	if throttle, err = pd.getInt32(); err == nil {
		c.ThrottleTime = time.Duration(throttle) * time.Millisecond
	}

	if version > 1 && err == nil {
		_, err = pd.getEmptyTaggedFieldArray()
	}

	return err
}

func (c *CreateDelegationTokenResponse) key() int16 {
	return 38
}

func (c *CreateDelegationTokenResponse) version() int16 {
	return c.Version
}

func (c *CreateDelegationTokenResponse) headerVersion() int16 {
	if c.Version > 1 {
		return 1
	}
	return 0
}

func (c *CreateDelegationTokenResponse) isValidVersion() bool {
	return c.Version >= 0 && c.Version <= 3
}

func (r *CreateDelegationTokenResponse) requiredVersion() KafkaVersion {
	switch r.Version {
	case 3:
		return V3_3_0_0
	default:
		return V1_1_0_0
	}
}

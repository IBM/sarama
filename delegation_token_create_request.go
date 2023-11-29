package sarama

import "time"

type CreateDelegationTokenRequest struct {
	Version            int16
	OwnerPrincipalType *string
	OwnerName          *string
	Renewers           []Principal
	MaxLifetime        time.Duration
}

func (c *CreateDelegationTokenRequest) encode(pe packetEncoder) (err error) {
	if c.Version > 2 {
		if err = pe.putNullableCompactString(c.OwnerPrincipalType); err != nil {
			return err
		}
		if err = pe.putNullableCompactString(c.OwnerName); err != nil {
			return err
		}
	}

	if c.Version > 1 {
		pe.putCompactArrayLength(len(c.Renewers))
	} else if err = pe.putArrayLength(len(c.Renewers)); err != nil {
		return err
	}

	for _, r := range c.Renewers {
		if err = r.encode(pe, c.Version); err != nil {
			return err
		}
		if c.Version > 1 {
			pe.putEmptyTaggedFieldArray()
		}
	}

	pe.putInt64(c.MaxLifetime.Milliseconds())

	if c.Version > 1 {
		pe.putEmptyTaggedFieldArray()
	}

	return nil
}

func (c *CreateDelegationTokenRequest) decode(pd packetDecoder, version int16) (err error) {
	c.Version = version

	if version > 2 {
		if c.OwnerPrincipalType, err = pd.getCompactNullableString(); err != nil {
			return err
		}
		if c.OwnerName, err = pd.getCompactNullableString(); err != nil {
			return err
		}
	}

	var n int
	if version > 1 {
		n, err = pd.getCompactArrayLength()
	} else {
		n, err = pd.getArrayLength()
	}
	if err != nil {
		return err
	}
	c.Renewers = make([]Principal, n)
	for i := range c.Renewers {
		if err := c.Renewers[i].decode(pd, version); err != nil {
			return err
		}
		if version > 1 {
			if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
				return err
			}
		}
	}

	var ms int64
	if ms, err = pd.getInt64(); err == nil {
		c.MaxLifetime = time.Duration(ms) * time.Millisecond
	}

	if version > 1 && err == nil {
		_, err = pd.getEmptyTaggedFieldArray()
	}

	return err
}

func (c *CreateDelegationTokenRequest) key() int16 {
	return 38
}

func (c *CreateDelegationTokenRequest) version() int16 {
	return c.Version
}

func (c *CreateDelegationTokenRequest) headerVersion() int16 {
	if c.Version > 1 {
		return 2
	}
	return 1
}

func (c *CreateDelegationTokenRequest) isValidVersion() bool {
	return c.Version >= 0 && c.Version <= 3
}

func (c *CreateDelegationTokenRequest) requiredVersion() KafkaVersion {
	switch c.Version {
	case 3:
		return V3_3_0_0
	default:
		return V1_1_0_0
	}
}

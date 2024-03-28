package sarama

import "time"

type DescribeDelegationTokenResponse struct {
	Version      int16
	ErrorCode    KError
	Tokens       []RenewableToken
	ThrottleTime time.Duration
}

type RenewableToken struct {
	DelegationToken
	Renewers []Principal
}

func (d *DescribeDelegationTokenResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(d.ErrorCode))

	if d.Version > 1 {
		pe.putCompactArrayLength(len(d.Tokens))
	} else if err := pe.putArrayLength(len(d.Tokens)); err != nil {
		return err
	}

	for _, t := range d.Tokens {
		if err := t.encode(pe, d.Version); err != nil {
			return err
		}

		if d.Version > 1 {
			pe.putCompactArrayLength(len(t.Renewers))
		} else if err := pe.putArrayLength(len(t.Renewers)); err != nil {
			return err
		}
		for _, r := range t.Renewers {
			if err := r.encode(pe, d.Version); err != nil {
				return err
			}
			if d.Version > 1 {
				pe.putEmptyTaggedFieldArray()
			}
		}

		if d.Version > 1 {
			pe.putEmptyTaggedFieldArray()
		}
	}

	pe.putInt32(int32(d.ThrottleTime / time.Millisecond))

	if d.Version > 1 {
		pe.putEmptyTaggedFieldArray()
	}

	return nil
}

func (d *DescribeDelegationTokenResponse) decode(pd packetDecoder, version int16) (err error) {
	d.Version = version

	if errCode, err := pd.getInt16(); err == nil {
		d.ErrorCode = KError(errCode)
	} else {
		return err
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

	d.Tokens = make([]RenewableToken, n)
	for i := range d.Tokens {
		if err := d.Tokens[i].decode(pd, version); err != nil {
			return err
		}

		if version > 1 {
			n, err = pd.getCompactArrayLength()
		} else {
			n, err = pd.getArrayLength()
		}
		if err != nil {
			return err
		}

		d.Tokens[i].Renewers = make([]Principal, n)
		for j := range d.Tokens[i].Renewers {
			if err = d.Tokens[i].Renewers[j].decode(pd, version); err != nil {
				return err
			}
			if d.Version > 1 {
				if _, err = pd.getEmptyTaggedFieldArray(); err != nil {
					return err
				}
			}
		}
		if d.Version > 1 {
			if _, err = pd.getEmptyTaggedFieldArray(); err != nil {
				return err
			}
		}
	}

	var throttle int32
	if throttle, err = pd.getInt32(); err == nil {
		d.ThrottleTime = time.Duration(throttle) * time.Millisecond
	}

	if d.Version > 1 && err == nil {
		_, err = pd.getEmptyTaggedFieldArray()
	}

	return err
}

func (d *DescribeDelegationTokenResponse) key() int16 {
	return 41
}

func (d *DescribeDelegationTokenResponse) version() int16 {
	return d.Version
}

func (d *DescribeDelegationTokenResponse) headerVersion() int16 {
	if d.Version > 1 {
		return 1
	}
	return 0
}

func (d *DescribeDelegationTokenResponse) isValidVersion() bool {
	return d.Version >= 0 && d.Version <= 3
}

func (d *DescribeDelegationTokenResponse) requiredVersion() KafkaVersion {
	return V1_1_0_0
}

package sarama

type DescribeDelegationTokenRequest struct {
	Version int16
	Owners  []Principal
}

func (d *DescribeDelegationTokenRequest) encode(pe packetEncoder) error {

	if d.Version > 1 {
		pe.putCompactArrayLength(len(d.Owners))
	} else if err := pe.putArrayLength(len(d.Owners)); err != nil {
		return err
	}

	for _, p := range d.Owners {
		if err := p.encode(pe, d.Version); err != nil {
			return err
		}
		if d.Version > 1 {
			pe.putEmptyTaggedFieldArray()
		}
	}

	if d.Version > 1 {
		pe.putEmptyTaggedFieldArray()
	}
	return nil
}

func (d *DescribeDelegationTokenRequest) decode(pd packetDecoder, version int16) (err error) {
	d.Version = version

	var n int
	if version > 1 {
		n, err = pd.getCompactArrayLength()
	} else {
		n, err = pd.getArrayLength()
	}
	if err != nil {
		return err
	}

	d.Owners = make([]Principal, n)
	for i := range d.Owners {
		if err = d.Owners[i].decode(pd, version); err != nil {
			return err
		}
		if version > 1 {
			if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
				return err
			}
		}
	}

	if d.Version > 1 {
		_, err = pd.getEmptyTaggedFieldArray()
	}

	return err
}

func (d *DescribeDelegationTokenRequest) key() int16 {
	return 41
}

func (d *DescribeDelegationTokenRequest) version() int16 {
	return d.Version
}

func (d *DescribeDelegationTokenRequest) headerVersion() int16 {
	if d.Version > 1 {
		return 2
	}
	return 1
}

func (d *DescribeDelegationTokenRequest) isValidVersion() bool {
	return d.Version >= 0 && d.Version <= 3
}

func (d *DescribeDelegationTokenRequest) requiredVersion() KafkaVersion {
	return V1_1_0_0
}

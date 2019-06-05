package sarama

//DescribeConfigsRequest is used to describe a config request
type DescribeConfigsRequest struct {
	Version         int16
	Resources       []*ConfigResource
	IncludeSynonyms bool
}

//ConfigResource is a config resource type
type ConfigResource struct {
	Type        ConfigResourceType
	Name        string
	ConfigNames []string
}

func (d *DescribeConfigsRequest) encode(pe packetEncoder) error {
	if err := pe.putArrayLength(len(d.Resources)); err != nil {
		return err
	}

	for _, c := range d.Resources {
		pe.putInt8(int8(c.Type))
		if err := pe.putString(c.Name); err != nil {
			return err
		}

		if len(c.ConfigNames) == 0 {
			pe.putInt32(-1)
			continue
		}
		if err := pe.putStringArray(c.ConfigNames); err != nil {
			return err
		}
	}

	if d.Version >= 1 {
		pe.putBool(d.IncludeSynonyms)
	}

	return nil
}

func (d *DescribeConfigsRequest) decode(pd packetDecoder, version int16) (err error) {
	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	d.Resources = make([]*ConfigResource, n)

	for i := 0; i < n; i++ {
		d.Resources[i] = &ConfigResource{}
		t, err := pd.getInt8()
		if err != nil {
			return err
		}
		d.Resources[i].Type = ConfigResourceType(t)
		name, err := pd.getString()
		if err != nil {
			return err
		}
		d.Resources[i].Name = name

		confLength, err := pd.getArrayLength()

		if err != nil {
			return err
		}

		if confLength == -1 {
			continue
		}

		cfnames := make([]string, confLength)
		for i := 0; i < confLength; i++ {
			s, err := pd.getString()
			if err != nil {
				return err
			}
			cfnames[i] = s
		}
		d.Resources[i].ConfigNames = cfnames
	}
	d.Version = version
	if d.Version >= 1 {
		b, err := pd.getBool()
		if err != nil {
			return err
		}
		d.IncludeSynonyms = b
	}

	return nil
}

func (d *DescribeConfigsRequest) key() int16 {
	return 32
}

func (d *DescribeConfigsRequest) version() int16 {
	return d.Version
}

func (d *DescribeConfigsRequest) requiredVersion() KafkaVersion {
	switch d.Version {
	case 1:
		return V1_1_0_0
	case 2:
		return V2_0_0_0
	default:
		return V0_11_0_0
	}
}

package sarama

type Resource struct {
	Type        ResourceType
	Name        string
	ConfigNames []string
}

type DescribeConfigsRequest struct {
	Resources []*Resource
}

func (r *DescribeConfigsRequest) encode(pe packetEncoder) error {
	if err := pe.putArrayLength(len(r.Resources)); err != nil {
		return err
	}

	for _, c := range r.Resources {
		pe.putInt8(int8(c.Type))
		if err := pe.putString(c.Name); err != nil {
			return err
		}
		if err := pe.putStringArray(c.ConfigNames); err != nil {
			return err
		}
	}

	return nil
}

func (r *DescribeConfigsRequest) decode(pd packetDecoder, version int16) (err error) {
	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	r.Resources = make([]*Resource, n)

	for i := 0; i < n; i++ {
		r.Resources[i] = &Resource{}
		t, err := pd.getInt8()
		if err != nil {
			return err
		}
		r.Resources[i].Type = ResourceType(t)
		name, err := pd.getString()
		if err != nil {
			return err
		}
		r.Resources[i].Name = name
		s, err := pd.getStringArray()
		if err != nil {
			return err
		}
		r.Resources[i].ConfigNames = s
	}

	return nil
}

func (r *DescribeConfigsRequest) key() int16 {
	return 32
}

func (r *DescribeConfigsRequest) version() int16 {
	return 0
}

func (r *DescribeConfigsRequest) requiredVersion() KafkaVersion {
	return V0_11_0_0
}

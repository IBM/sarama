package sarama

type AlterConfigsRequest struct {
	Resources    []*AlterConfigsResource
	ValidateOnly bool
}

type AlterConfigsResource struct {
	T             ResourceType
	Name          string
	ConfigEntries []*ConfigEntryKV
}

type ConfigEntryKV struct {
	Name  string
	Value string
}

func (acr *AlterConfigsRequest) encode(pe packetEncoder) error {
	if err := pe.putArrayLength(len(acr.Resources)); err != nil {
		return err
	}

	for _, r := range acr.Resources {
		if err := r.encode(pe); err != nil {
			return err
		}
	}

	pe.putBool(acr.ValidateOnly)
	return nil
}

func (acr *AlterConfigsRequest) decode(pd packetDecoder, version int16) error {
	resourceCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	acr.Resources = make([]*AlterConfigsResource, resourceCount)
	for i := range acr.Resources {
		r := &AlterConfigsResource{}
		err = r.decode(pd, version)
		if err != nil {
			return err
		}
		acr.Resources[i] = r
	}

	validateOnly, err := pd.getBool()
	if err != nil {
		return err
	}

	acr.ValidateOnly = validateOnly

	return nil
}

func (ac *AlterConfigsResource) encode(pe packetEncoder) error {
	pe.putInt8(int8(ac.T))

	if err := pe.putString(ac.Name); err != nil {
		return err
	}

	if err := pe.putArrayLength(len(ac.ConfigEntries)); err != nil {
		return err
	}

	for _, r := range ac.ConfigEntries {
		if err := r.encode(pe); err != nil {
			return err
		}
	}

	return nil
}

func (ac *AlterConfigsResource) decode(pd packetDecoder, version int16) error {
	t, err := pd.getInt8()
	if err != nil {
		return err
	}
	ac.T = ResourceType(t)

	name, err := pd.getString()
	if err != nil {
		return err
	}
	ac.Name = name

	configCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	ac.ConfigEntries = make([]*ConfigEntryKV, configCount)
	for i, _ := range ac.ConfigEntries {
		r := &ConfigEntryKV{}
		if err := r.decode(pd, version); err != nil {
			return err
		}
		ac.ConfigEntries[i] = r
	}

	return err
}

func (acr *AlterConfigsRequest) key() int16 {
	return 33
}

func (acr *AlterConfigsRequest) version() int16 {
	return 0
}

func (acr *AlterConfigsRequest) requiredVersion() KafkaVersion {
	return V0_11_0_0
}

func (c *ConfigEntryKV) encode(pe packetEncoder) error {
	if err := pe.putString(c.Name); err != nil {
		return err
	}
	if err := pe.putString(c.Value); err != nil {
		return err
	}
	return nil
}

func (c *ConfigEntryKV) decode(pe packetDecoder, version int16) error {
	name, err := pe.getString()
	if err != nil {
		return err
	}
	c.Name = name

	value, err := pe.getString()
	if err != nil {
		return err
	}
	c.Value = value

	return nil
}

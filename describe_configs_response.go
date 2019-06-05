package sarama

import (
	"fmt"
	"time"
)

//ConfigSource is used to configure source
type ConfigSource int8

//String implement stringer interface
func (s ConfigSource) String() string {
	switch s {
	case SourceUnknown:
		return "Unknown"
	case SourceTopic:
		return "Topic"
	case SourceDynamicBroker:
		return "DynamicBroker"
	case SourceDynamicDefaultBroker:
		return "DynamicDefaultBroker"
	case SourceStaticBroker:
		return "StaticBroker"
	case SourceDefault:
		return "Default"
	}
	return fmt.Sprintf("Source Invalid: %d", int(s))
}

const (
	//SourceUnknown is unknow config source
	SourceUnknown ConfigSource = iota
	//SourceTopic is a topic source
	SourceTopic
	//SourceDynamicBroker is a dynamic broker type
	SourceDynamicBroker
	//SourceDynamicDefaultBroker is default broker type
	SourceDynamicDefaultBroker
	//SourceStaticBroker is a static broker type
	SourceStaticBroker
	//SourceDefault is a default source
	SourceDefault
)

//DescribeConfigsResponse is used to describe config response
type DescribeConfigsResponse struct {
	Version      int16
	ThrottleTime time.Duration
	Resources    []*ResourceResponse
}

//ResourceResponse is a resource response type
type ResourceResponse struct {
	ErrorCode int16
	ErrorMsg  string
	Type      ConfigResourceType
	Name      string
	Configs   []*ConfigEntry
}

//ConfigEntry is an entry in config
type ConfigEntry struct {
	Name      string
	Value     string
	ReadOnly  bool
	Default   bool
	Source    ConfigSource
	Sensitive bool
	Synonyms  []*ConfigSynonym
}

//ConfigSynonym is a synonym for config
type ConfigSynonym struct {
	ConfigName  string
	ConfigValue string
	Source      ConfigSource
}

func (d *DescribeConfigsResponse) encode(pe packetEncoder) (err error) {
	pe.putInt32(int32(d.ThrottleTime / time.Millisecond))
	if err = pe.putArrayLength(len(d.Resources)); err != nil {
		return err
	}

	for _, c := range d.Resources {
		if err = c.encode(pe, d.Version); err != nil {
			return err
		}
	}

	return nil
}

func (d *DescribeConfigsResponse) decode(pd packetDecoder, version int16) (err error) {
	d.Version = version
	throttleTime, err := pd.getInt32()
	if err != nil {
		return err
	}
	d.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	d.Resources = make([]*ResourceResponse, n)
	for i := 0; i < n; i++ {
		rr := &ResourceResponse{}
		if err := rr.decode(pd, version); err != nil {
			return err
		}
		d.Resources[i] = rr
	}

	return nil
}

func (d *DescribeConfigsResponse) key() int16 {
	return 32
}

func (d *DescribeConfigsResponse) version() int16 {
	return d.Version
}

func (d *DescribeConfigsResponse) requiredVersion() KafkaVersion {
	switch d.Version {
	case 1:
		return V1_0_0_0
	case 2:
		return V2_0_0_0
	default:
		return V0_11_0_0
	}
}

func (r *ResourceResponse) encode(pe packetEncoder, version int16) (err error) {
	pe.putInt16(r.ErrorCode)

	if err = pe.putString(r.ErrorMsg); err != nil {
		return err
	}

	pe.putInt8(int8(r.Type))

	if err = pe.putString(r.Name); err != nil {
		return err
	}

	if err = pe.putArrayLength(len(r.Configs)); err != nil {
		return err
	}

	for _, c := range r.Configs {
		if err = c.encode(pe, version); err != nil {
			return err
		}
	}
	return nil
}

func (r *ResourceResponse) decode(pd packetDecoder, version int16) (err error) {
	ec, err := pd.getInt16()
	if err != nil {
		return err
	}
	r.ErrorCode = ec

	em, err := pd.getString()
	if err != nil {
		return err
	}
	r.ErrorMsg = em

	t, err := pd.getInt8()
	if err != nil {
		return err
	}
	r.Type = ConfigResourceType(t)

	name, err := pd.getString()
	if err != nil {
		return err
	}
	r.Name = name

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	r.Configs = make([]*ConfigEntry, n)
	for i := 0; i < n; i++ {
		c := &ConfigEntry{}
		if err := c.decode(pd, version); err != nil {
			return err
		}
		r.Configs[i] = c
	}
	return nil
}

func (c *ConfigEntry) encode(pe packetEncoder, version int16) (err error) {
	if err = pe.putString(c.Name); err != nil {
		return err
	}

	if err = pe.putString(c.Value); err != nil {
		return err
	}

	pe.putBool(c.ReadOnly)

	if version <= 0 {
		pe.putBool(c.Default)
		pe.putBool(c.Sensitive)
	} else {
		pe.putInt8(int8(c.Source))
		pe.putBool(c.Sensitive)

		if err := pe.putArrayLength(len(c.Synonyms)); err != nil {
			return err
		}
		for _, s := range c.Synonyms {
			if err = s.encode(pe, version); err != nil {
				return err
			}
		}
	}

	return nil
}

//https://cwiki.apache.org/confluence/display/KAFKA/KIP-226+-+Dynamic+Broker+Configuration
func (c *ConfigEntry) decode(pd packetDecoder, version int16) (err error) {
	if version == 0 {
		c.Source = SourceUnknown
	}
	name, err := pd.getString()
	if err != nil {
		return err
	}
	c.Name = name

	value, err := pd.getString()
	if err != nil {
		return err
	}
	c.Value = value

	read, err := pd.getBool()
	if err != nil {
		return err
	}
	c.ReadOnly = read

	if version == 0 {
		defaultB, err := pd.getBool()
		if err != nil {
			return err
		}
		c.Default = defaultB
	} else {
		source, err := pd.getInt8()
		if err != nil {
			return err
		}
		c.Source = ConfigSource(source)
	}

	sensitive, err := pd.getBool()
	if err != nil {
		return err
	}
	c.Sensitive = sensitive

	if version > 0 {
		n, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		c.Synonyms = make([]*ConfigSynonym, n)

		for i := 0; i < n; i++ {
			s := &ConfigSynonym{}
			if err := s.decode(pd, version); err != nil {
				return err
			}
			c.Synonyms[i] = s
		}

	}
	return nil
}

func (c *ConfigSynonym) encode(pe packetEncoder, version int16) (err error) {
	err = pe.putString(c.ConfigName)
	if err != nil {
		return err
	}

	err = pe.putString(c.ConfigValue)
	if err != nil {
		return err
	}

	pe.putInt8(int8(c.Source))

	return nil
}

func (c *ConfigSynonym) decode(pd packetDecoder, version int16) error {
	name, err := pd.getString()
	if err != nil {
		return nil
	}
	c.ConfigName = name

	value, err := pd.getString()
	if err != nil {
		return nil
	}
	c.ConfigValue = value

	source, err := pd.getInt8()
	if err != nil {
		return nil
	}
	c.Source = ConfigSource(source)
	return nil
}

package sarama

//CreateACLsRequest is an acl creation request
type CreateACLsRequest struct {
	Version      int16
	ACLCreations []*ACLCreation
}

func (c *CreateACLsRequest) encode(pe packetEncoder) error {
	if err := pe.putArrayLength(len(c.ACLCreations)); err != nil {
		return err
	}

	for _, aclCreation := range c.ACLCreations {
		if err := aclCreation.encode(pe, c.Version); err != nil {
			return err
		}
	}

	return nil
}

func (c *CreateACLsRequest) decode(pd packetDecoder, version int16) (err error) {
	c.Version = version
	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	c.ACLCreations = make([]*ACLCreation, n)

	for i := 0; i < n; i++ {
		c.ACLCreations[i] = new(ACLCreation)
		if err := c.ACLCreations[i].decode(pd, version); err != nil {
			return err
		}
	}

	return nil
}

func (c *CreateACLsRequest) key() int16 {
	return 30
}

func (c *CreateACLsRequest) version() int16 {
	return c.Version
}

func (c *CreateACLsRequest) requiredVersion() KafkaVersion {
	switch c.Version {
	case 1:
		return V2_0_0_0
	default:
		return V0_11_0_0
	}
}

//ACLCreation is a wrapper around Resource and ACL type
type ACLCreation struct {
	Resource
	ACL
}

func (a *ACLCreation) encode(pe packetEncoder, version int16) error {
	if err := a.Resource.encode(pe, version); err != nil {
		return err
	}
	if err := a.ACL.encode(pe); err != nil {
		return err
	}

	return nil
}

func (a *ACLCreation) decode(pd packetDecoder, version int16) (err error) {
	if err := a.Resource.decode(pd, version); err != nil {
		return err
	}
	if err := a.ACL.decode(pd, version); err != nil {
		return err
	}

	return nil
}

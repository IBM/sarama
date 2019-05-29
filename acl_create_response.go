package sarama

import "time"

//CreateACLsResponse is a an acl reponse creation type
type CreateACLsResponse struct {
	ThrottleTime         time.Duration
	ACLCreationResponses []*ACLCreationResponse
}

func (c *CreateACLsResponse) encode(pe packetEncoder) error {
	pe.putInt32(int32(c.ThrottleTime / time.Millisecond))

	if err := pe.putArrayLength(len(c.ACLCreationResponses)); err != nil {
		return err
	}

	for _, aclCreationResponse := range c.ACLCreationResponses {
		if err := aclCreationResponse.encode(pe); err != nil {
			return err
		}
	}

	return nil
}

func (c *CreateACLsResponse) decode(pd packetDecoder, version int16) (err error) {
	throttleTime, err := pd.getInt32()
	if err != nil {
		return err
	}
	c.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	c.ACLCreationResponses = make([]*ACLCreationResponse, n)
	for i := 0; i < n; i++ {
		c.ACLCreationResponses[i] = new(ACLCreationResponse)
		if err := c.ACLCreationResponses[i].decode(pd, version); err != nil {
			return err
		}
	}

	return nil
}

func (c *CreateACLsResponse) key() int16 {
	return 30
}

func (c *CreateACLsResponse) version() int16 {
	return 0
}

func (c *CreateACLsResponse) requiredVersion() KafkaVersion {
	return V0_11_0_0
}

//ACLCreationResponse is an acl creation response type
type ACLCreationResponse struct {
	Err    KError
	ErrMsg *string
}

func (a *ACLCreationResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(a.Err))

	if err := pe.putNullableString(a.ErrMsg); err != nil {
		return err
	}

	return nil
}

func (a *ACLCreationResponse) decode(pd packetDecoder, version int16) (err error) {
	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}
	a.Err = KError(kerr)

	if a.ErrMsg, err = pd.getNullableString(); err != nil {
		return err
	}

	return nil
}

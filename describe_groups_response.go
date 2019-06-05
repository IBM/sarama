package sarama

//DescribeGroupsResponse is used to describe groups response
type DescribeGroupsResponse struct {
	Groups []*GroupDescription
}

func (d *DescribeGroupsResponse) encode(pe packetEncoder) error {
	if err := pe.putArrayLength(len(d.Groups)); err != nil {
		return err
	}

	for _, groupDescription := range d.Groups {
		if err := groupDescription.encode(pe); err != nil {
			return err
		}
	}

	return nil
}

func (d *DescribeGroupsResponse) decode(pd packetDecoder, version int16) (err error) {
	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	d.Groups = make([]*GroupDescription, n)
	for i := 0; i < n; i++ {
		d.Groups[i] = new(GroupDescription)
		if err := d.Groups[i].decode(pd); err != nil {
			return err
		}
	}

	return nil
}

func (d *DescribeGroupsResponse) key() int16 {
	return 15
}

func (d *DescribeGroupsResponse) version() int16 {
	return 0
}

func (d *DescribeGroupsResponse) requiredVersion() KafkaVersion {
	return V0_9_0_0
}

//GroupDescription describes a group
type GroupDescription struct {
	Err          KError
	GroupID      string
	State        string
	ProtocolType string
	Protocol     string
	Members      map[string]*GroupMemberDescription
}

func (g *GroupDescription) encode(pe packetEncoder) error {
	pe.putInt16(int16(g.Err))

	if err := pe.putString(g.GroupID); err != nil {
		return err
	}
	if err := pe.putString(g.State); err != nil {
		return err
	}
	if err := pe.putString(g.ProtocolType); err != nil {
		return err
	}
	if err := pe.putString(g.Protocol); err != nil {
		return err
	}

	if err := pe.putArrayLength(len(g.Members)); err != nil {
		return err
	}

	for memberID, groupMemberDescription := range g.Members {
		if err := pe.putString(memberID); err != nil {
			return err
		}
		if err := groupMemberDescription.encode(pe); err != nil {
			return err
		}
	}

	return nil
}

func (g *GroupDescription) decode(pd packetDecoder) (err error) {
	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}

	g.Err = KError(kerr)

	if g.GroupID, err = pd.getString(); err != nil {
		return
	}
	if g.State, err = pd.getString(); err != nil {
		return
	}
	if g.ProtocolType, err = pd.getString(); err != nil {
		return
	}
	if g.Protocol, err = pd.getString(); err != nil {
		return
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	g.Members = make(map[string]*GroupMemberDescription)
	for i := 0; i < n; i++ {
		memberID, err := pd.getString()
		if err != nil {
			return err
		}

		g.Members[memberID] = new(GroupMemberDescription)
		if err := g.Members[memberID].decode(pd); err != nil {
			return err
		}
	}

	return nil
}

//GroupMemberDescription is a group member description
type GroupMemberDescription struct {
	ClientID         string
	ClientHost       string
	MemberMetadata   []byte
	MemberAssignment []byte
}

func (g *GroupMemberDescription) encode(pe packetEncoder) error {
	if err := pe.putString(g.ClientID); err != nil {
		return err
	}
	if err := pe.putString(g.ClientHost); err != nil {
		return err
	}
	if err := pe.putBytes(g.MemberMetadata); err != nil {
		return err
	}
	if err := pe.putBytes(g.MemberAssignment); err != nil {
		return err
	}

	return nil
}

func (g *GroupMemberDescription) decode(pd packetDecoder) (err error) {
	if g.ClientID, err = pd.getString(); err != nil {
		return
	}
	if g.ClientHost, err = pd.getString(); err != nil {
		return
	}
	if g.MemberMetadata, err = pd.getBytes(); err != nil {
		return
	}
	if g.MemberAssignment, err = pd.getBytes(); err != nil {
		return
	}

	return nil
}

//GetMemberAssignment returns member assignment
func (g *GroupMemberDescription) GetMemberAssignment() (*ConsumerGroupMemberAssignment, error) {
	assignment := new(ConsumerGroupMemberAssignment)
	err := decode(g.MemberAssignment, assignment)
	return assignment, err
}

//GetMemberMetadata returns member metadata
func (g *GroupMemberDescription) GetMemberMetadata() (*ConsumerGroupMemberMetadata, error) {
	metadata := new(ConsumerGroupMemberMetadata)
	err := decode(g.MemberMetadata, metadata)
	return metadata, err
}

package sarama

type DescribeGroupsResponse struct {
	Version              int16
	ThrottleTimeMs       int32
	Groups               []*GroupDescription
	AuthorizedOperations int32
}

func (r *DescribeGroupsResponse) encode(pe packetEncoder) error {
	if r.Version >= 1 {
		pe.putInt32(r.ThrottleTimeMs)
	}
	if err := pe.putArrayLength(len(r.Groups)); err != nil {
		return err
	}

	for _, groupDescription := range r.Groups {
		groupDescription.Version = r.Version
		if err := groupDescription.encode(pe); err != nil {
			return err
		}
	}
	if r.Version >= 3 {
		pe.putInt32(r.AuthorizedOperations)
	}

	return nil
}

func (r *DescribeGroupsResponse) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version
	if r.Version >= 1 {
		if r.ThrottleTimeMs, err = pd.getInt32(); err != nil {
			return err
		}
	}
	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	r.Groups = make([]*GroupDescription, n)
	for i := 0; i < n; i++ {
		r.Groups[i] = new(GroupDescription)
		r.Groups[i].Version = r.Version
		if err := r.Groups[i].decode(pd); err != nil {
			return err
		}
	}
	if r.Version >= 3 {
		if r.AuthorizedOperations, err = pd.getInt32(); err != nil {
			return err
		}
	}

	return nil
}

func (r *DescribeGroupsResponse) key() int16 {
	return 15
}

func (r *DescribeGroupsResponse) version() int16 {
	return r.Version
}

func (r *DescribeGroupsResponse) headerVersion() int16 {
	return 0
}

func (r *DescribeGroupsResponse) requiredVersion() KafkaVersion {
	switch r.Version {
	case 1, 2, 3, 4:
		return V2_3_0_0
	}
	return V0_9_0_0
}

type GroupDescription struct {
	Version int16

	Err          KError
	GroupId      string
	State        string
	ProtocolType string
	Protocol     string
	Members      map[string]*GroupMemberDescription
}

func (gd *GroupDescription) encode(pe packetEncoder) error {
	pe.putInt16(int16(gd.Err))

	if err := pe.putString(gd.GroupId); err != nil {
		return err
	}
	if err := pe.putString(gd.State); err != nil {
		return err
	}
	if err := pe.putString(gd.ProtocolType); err != nil {
		return err
	}
	if err := pe.putString(gd.Protocol); err != nil {
		return err
	}

	if err := pe.putArrayLength(len(gd.Members)); err != nil {
		return err
	}

	for memberId, groupMemberDescription := range gd.Members {
		if err := pe.putString(memberId); err != nil {
			return err
		}
		// encode with version
		groupMemberDescription.Version = gd.Version
		if err := groupMemberDescription.encode(pe); err != nil {
			return err
		}
	}

	return nil
}

func (gd *GroupDescription) decode(pd packetDecoder) (err error) {
	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}

	gd.Err = KError(kerr)

	if gd.GroupId, err = pd.getString(); err != nil {
		return
	}
	if gd.State, err = pd.getString(); err != nil {
		return
	}
	if gd.ProtocolType, err = pd.getString(); err != nil {
		return
	}
	if gd.Protocol, err = pd.getString(); err != nil {
		return
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	gd.Members = make(map[string]*GroupMemberDescription)
	for i := 0; i < n; i++ {
		memberId, err := pd.getString()
		if err != nil {
			return err
		}

		gd.Members[memberId] = new(GroupMemberDescription)
		gd.Members[memberId].Version = gd.Version
		if err := gd.Members[memberId].decode(pd); err != nil {
			return err
		}
	}

	return nil
}

type GroupMemberDescription struct {
	Version int16

	GroupInstanceId  *string
	ClientId         string
	ClientHost       string
	MemberMetadata   []byte
	MemberAssignment []byte
}

func (gmd *GroupMemberDescription) encode(pe packetEncoder) error {
	if gmd.Version >= 4 {
		if err := pe.putNullableString(gmd.GroupInstanceId); err != nil {
			return err
		}
	}
	if err := pe.putString(gmd.ClientId); err != nil {
		return err
	}
	if err := pe.putString(gmd.ClientHost); err != nil {
		return err
	}
	if err := pe.putBytes(gmd.MemberMetadata); err != nil {
		return err
	}
	if err := pe.putBytes(gmd.MemberAssignment); err != nil {
		return err
	}

	return nil
}

func (gmd *GroupMemberDescription) decode(pd packetDecoder) (err error) {
	if gmd.Version >= 4 {
		if gmd.GroupInstanceId, err = pd.getNullableString(); err != nil {
			return
		}
	}
	if gmd.ClientId, err = pd.getString(); err != nil {
		return
	}
	if gmd.ClientHost, err = pd.getString(); err != nil {
		return
	}
	if gmd.MemberMetadata, err = pd.getBytes(); err != nil {
		return
	}
	if gmd.MemberAssignment, err = pd.getBytes(); err != nil {
		return
	}

	return nil
}

func (gmd *GroupMemberDescription) GetMemberAssignment() (*ConsumerGroupMemberAssignment, error) {
	if len(gmd.MemberAssignment) == 0 {
		return nil, nil
	}
	assignment := new(ConsumerGroupMemberAssignment)
	err := decode(gmd.MemberAssignment, assignment)
	return assignment, err
}

func (gmd *GroupMemberDescription) GetMemberMetadata() (*ConsumerGroupMemberMetadata, error) {
	if len(gmd.MemberMetadata) == 0 {
		return nil, nil
	}
	metadata := new(ConsumerGroupMemberMetadata)
	err := decode(gmd.MemberMetadata, metadata)
	return metadata, err
}

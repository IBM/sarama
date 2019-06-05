package sarama

//JoinGroupResponse is a join group response
type JoinGroupResponse struct {
	Version       int16
	ThrottleTime  int32
	Err           KError
	GenerationID  int32
	GroupProtocol string
	LeaderID      string
	MemberID      string
	Members       map[string][]byte
}

//GetMembers returns members from join group response
func (j *JoinGroupResponse) GetMembers() (map[string]ConsumerGroupMemberMetadata, error) {
	members := make(map[string]ConsumerGroupMemberMetadata, len(j.Members))
	for id, bin := range j.Members {
		meta := new(ConsumerGroupMemberMetadata)
		if err := decode(bin, meta); err != nil {
			return nil, err
		}
		members[id] = *meta
	}
	return members, nil
}

func (j *JoinGroupResponse) encode(pe packetEncoder) error {
	if j.Version >= 2 {
		pe.putInt32(j.ThrottleTime)
	}
	pe.putInt16(int16(j.Err))
	pe.putInt32(j.GenerationID)

	if err := pe.putString(j.GroupProtocol); err != nil {
		return err
	}
	if err := pe.putString(j.LeaderID); err != nil {
		return err
	}
	if err := pe.putString(j.MemberID); err != nil {
		return err
	}

	if err := pe.putArrayLength(len(j.Members)); err != nil {
		return err
	}

	for memberID, memberMetadata := range j.Members {
		if err := pe.putString(memberID); err != nil {
			return err
		}

		if err := pe.putBytes(memberMetadata); err != nil {
			return err
		}
	}

	return nil
}

func (j *JoinGroupResponse) decode(pd packetDecoder, version int16) (err error) {
	j.Version = version

	if version >= 2 {
		if j.ThrottleTime, err = pd.getInt32(); err != nil {
			return
		}
	}

	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}

	j.Err = KError(kerr)

	if j.GenerationID, err = pd.getInt32(); err != nil {
		return
	}

	if j.GroupProtocol, err = pd.getString(); err != nil {
		return
	}

	if j.LeaderID, err = pd.getString(); err != nil {
		return
	}

	if j.MemberID, err = pd.getString(); err != nil {
		return
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	j.Members = make(map[string][]byte)
	for i := 0; i < n; i++ {
		memberID, err := pd.getString()
		if err != nil {
			return err
		}

		memberMetadata, err := pd.getBytes()
		if err != nil {
			return err
		}

		j.Members[memberID] = memberMetadata
	}

	return nil
}

func (j *JoinGroupResponse) key() int16 {
	return 11
}

func (j *JoinGroupResponse) version() int16 {
	return j.Version
}

func (j *JoinGroupResponse) requiredVersion() KafkaVersion {
	switch j.Version {
	case 2:
		return V0_11_0_0
	case 1:
		return V0_10_1_0
	default:
		return V0_9_0_0
	}
}

package sarama

type SyncGroupRequest struct {
	Version          int16
	GroupId          string
	GenerationId     int32
	MemberId         string
	GroupInstanceId  *string
	GroupAssignments map[string][]byte
}

func (r *SyncGroupRequest) encode(pe packetEncoder) error {
	if err := pe.putString(r.GroupId); err != nil {
		return err
	}

	pe.putInt32(r.GenerationId)

	if err := pe.putString(r.MemberId); err != nil {
		return err
	}

	if r.Version >= 3 {
		if err := pe.putNullableString(r.GroupInstanceId); err != nil {
			return err
		}
	}

	if err := pe.putArrayLength(len(r.GroupAssignments)); err != nil {
		return err
	}
	for memberId, memberAssignment := range r.GroupAssignments {
		if err := pe.putString(memberId); err != nil {
			return err
		}
		if err := pe.putBytes(memberAssignment); err != nil {
			return err
		}
	}

	return nil
}

func (r *SyncGroupRequest) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version

	if r.GroupId, err = pd.getString(); err != nil {
		return
	}
	if r.GenerationId, err = pd.getInt32(); err != nil {
		return
	}
	if r.MemberId, err = pd.getString(); err != nil {
		return
	}
	if r.Version >= 3 {
		if r.GroupInstanceId, err = pd.getNullableString(); err != nil {
			return
		}
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	r.GroupAssignments = make(map[string][]byte)
	for i := 0; i < n; i++ {
		memberId, err := pd.getString()
		if err != nil {
			return err
		}
		memberAssignment, err := pd.getBytes()
		if err != nil {
			return err
		}

		r.GroupAssignments[memberId] = memberAssignment
	}

	return nil
}

func (r *SyncGroupRequest) key() int16 {
	return 14
}

func (r *SyncGroupRequest) version() int16 {
	return r.Version
}

func (r *SyncGroupRequest) headerVersion() int16 {
	return 1
}

func (r *SyncGroupRequest) requiredVersion() KafkaVersion {
	switch {
	case r.Version >= 3:
		return V2_3_0_0
	}
	return V0_9_0_0
}

func (r *SyncGroupRequest) AddGroupAssignment(memberId string, memberAssignment []byte) {
	if r.GroupAssignments == nil {
		r.GroupAssignments = make(map[string][]byte)
	}

	r.GroupAssignments[memberId] = memberAssignment
}

func (r *SyncGroupRequest) AddGroupAssignmentMember(memberId string, memberAssignment *ConsumerGroupMemberAssignment) error {
	bin, err := encode(memberAssignment, nil)
	if err != nil {
		return err
	}

	r.AddGroupAssignment(memberId, bin)
	return nil
}

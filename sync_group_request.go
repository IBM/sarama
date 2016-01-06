package sarama

type SyncGroupRequest struct {
	GroupId         string
	GenerationId    int32
	MemberId        string
	GroupAssignment map[string][]byte
}

func (r *SyncGroupRequest) encode(pe packetEncoder) error {
	if err := pe.putString(r.GroupId); err != nil {
		return err
	}

	pe.putInt32(int32(r.GenerationId))

	if err := pe.putString(r.MemberId); err != nil {
		return err
	}

	if err := pe.putArrayLength(len(r.GroupAssignment)); err != nil {
		return err
	}
	for memberId, memberAssignment := range r.GroupAssignment {
		if err := pe.putString(memberId); err != nil {
			return err
		}

		if err := pe.putBytes(memberAssignment); err != nil {
			return err
		}
	}

	return nil
}

func (r *SyncGroupRequest) decode(pd packetDecoder) (err error) {
	if r.GroupId, err = pd.getString(); err != nil {
		return
	}
	if r.GenerationId, err = pd.getInt32(); err != nil {
		return
	}
	if r.MemberId, err = pd.getString(); err != nil {
		return
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	r.GroupAssignment = make(map[string][]byte, n)
	for i := 0; i < n; i++ {
		memberId, err := pd.getString()
		if err != nil {
			return err
		}

		gaBytes, err := pd.getBytes()
		if err != nil {
			return err
		}

		r.GroupAssignment[memberId] = gaBytes
	}

	return nil
}

func (r *SyncGroupRequest) key() int16 {
	return 14
}

func (r *SyncGroupRequest) version() int16 {
	return 0
}

func (r *SyncGroupRequest) AddGroupAssignment(memberId string, memberAssignment []byte) {
	if r.GroupAssignment == nil {
		r.GroupAssignment = make(map[string][]byte)
	}

	r.GroupAssignment[memberId] = memberAssignment
}

func (r *SyncGroupRequest) AddGroupAssignmentMember(memberId string, memberAssignment *ConsumerGroupMemberAssignment) error {
	bin, err := encode(memberAssignment)
	if err != nil {
		return err
	}

	r.AddGroupAssignment(memberId, bin)
	return nil
}

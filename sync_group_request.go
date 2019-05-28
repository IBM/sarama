package sarama

type SyncGroupRequest struct {
	GroupID          string
	GenerationID     int32
	MemberID         string
	GroupAssignments map[string][]byte
}

func (r *SyncGroupRequest) encode(pe packetEncoder) error {
	if err := pe.putString(r.GroupID); err != nil {
		return err
	}

	pe.putInt32(r.GenerationID)

	if err := pe.putString(r.MemberID); err != nil {
		return err
	}

	if err := pe.putArrayLength(len(r.GroupAssignments)); err != nil {
		return err
	}
	for memberID, memberAssignment := range r.GroupAssignments {
		if err := pe.putString(memberID); err != nil {
			return err
		}
		if err := pe.putBytes(memberAssignment); err != nil {
			return err
		}
	}

	return nil
}

func (r *SyncGroupRequest) decode(pd packetDecoder, version int16) (err error) {
	if r.GroupID, err = pd.getString(); err != nil {
		return
	}
	if r.GenerationID, err = pd.getInt32(); err != nil {
		return
	}
	if r.MemberID, err = pd.getString(); err != nil {
		return
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
		memberID, err := pd.getString()
		if err != nil {
			return err
		}
		memberAssignment, err := pd.getBytes()
		if err != nil {
			return err
		}

		r.GroupAssignments[memberID] = memberAssignment
	}

	return nil
}

func (r *SyncGroupRequest) key() int16 {
	return 14
}

func (r *SyncGroupRequest) version() int16 {
	return 0
}

func (r *SyncGroupRequest) requiredVersion() KafkaVersion {
	return V0_9_0_0
}

func (r *SyncGroupRequest) AddGroupAssignment(memberID string, memberAssignment []byte) {
	if r.GroupAssignments == nil {
		r.GroupAssignments = make(map[string][]byte)
	}

	r.GroupAssignments[memberID] = memberAssignment
}

func (r *SyncGroupRequest) AddGroupAssignmentMember(memberID string, memberAssignment *ConsumerGroupMemberAssignment) error {
	bin, err := encode(memberAssignment, nil)
	if err != nil {
		return err
	}

	r.AddGroupAssignment(memberID, bin)
	return nil
}

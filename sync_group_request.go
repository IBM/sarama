package sarama

//SyncGroupRequest is a sync group request
type SyncGroupRequest struct {
	GroupID          string
	GenerationID     int32
	MemberID         string
	GroupAssignments map[string][]byte
}

func (s *SyncGroupRequest) encode(pe packetEncoder) error {
	if err := pe.putString(s.GroupID); err != nil {
		return err
	}

	pe.putInt32(s.GenerationID)

	if err := pe.putString(s.MemberID); err != nil {
		return err
	}

	if err := pe.putArrayLength(len(s.GroupAssignments)); err != nil {
		return err
	}
	for memberID, memberAssignment := range s.GroupAssignments {
		if err := pe.putString(memberID); err != nil {
			return err
		}
		if err := pe.putBytes(memberAssignment); err != nil {
			return err
		}
	}

	return nil
}

func (s *SyncGroupRequest) decode(pd packetDecoder, version int16) (err error) {
	if s.GroupID, err = pd.getString(); err != nil {
		return
	}
	if s.GenerationID, err = pd.getInt32(); err != nil {
		return
	}
	if s.MemberID, err = pd.getString(); err != nil {
		return
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	s.GroupAssignments = make(map[string][]byte)
	for i := 0; i < n; i++ {
		memberID, err := pd.getString()
		if err != nil {
			return err
		}
		memberAssignment, err := pd.getBytes()
		if err != nil {
			return err
		}

		s.GroupAssignments[memberID] = memberAssignment
	}

	return nil
}

func (s *SyncGroupRequest) key() int16 {
	return 14
}

func (s *SyncGroupRequest) version() int16 {
	return 0
}

func (s *SyncGroupRequest) requiredVersion() KafkaVersion {
	return V0_9_0_0
}

//AddGroupAssignment adds group member assignment
func (s *SyncGroupRequest) AddGroupAssignment(memberID string, memberAssignment []byte) {
	if s.GroupAssignments == nil {
		s.GroupAssignments = make(map[string][]byte)
	}

	s.GroupAssignments[memberID] = memberAssignment
}

//AddGroupAssignmentMember adds group member assignment member
func (s *SyncGroupRequest) AddGroupAssignmentMember(memberID string, memberAssignment *ConsumerGroupMemberAssignment) error {
	bin, err := encode(memberAssignment, nil)
	if err != nil {
		return err
	}

	s.AddGroupAssignment(memberID, bin)
	return nil
}

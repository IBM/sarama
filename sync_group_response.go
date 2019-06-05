package sarama

//SyncGroupResponse is a sync group response
type SyncGroupResponse struct {
	Err              KError
	MemberAssignment []byte
}

//GetMemberAssignment returns consumer group memeber assignment
func (s *SyncGroupResponse) GetMemberAssignment() (*ConsumerGroupMemberAssignment, error) {
	assignment := new(ConsumerGroupMemberAssignment)
	err := decode(s.MemberAssignment, assignment)
	return assignment, err
}

func (s *SyncGroupResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(s.Err))
	return pe.putBytes(s.MemberAssignment)
}

func (s *SyncGroupResponse) decode(pd packetDecoder, version int16) (err error) {
	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}

	s.Err = KError(kerr)

	s.MemberAssignment, err = pd.getBytes()
	return
}

func (s *SyncGroupResponse) key() int16 {
	return 14
}

func (s *SyncGroupResponse) version() int16 {
	return 0
}

func (s *SyncGroupResponse) requiredVersion() KafkaVersion {
	return V0_9_0_0
}

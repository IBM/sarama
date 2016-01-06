package sarama

type SyncGroupResponse struct {
	Err              KError
	MemberAssignment []byte
}

func (r *SyncGroupResponse) GetMemberAssignment() (*ConsumerGroupMemberAssignment, error) {
	assignment := new(ConsumerGroupMemberAssignment)
	err := decode(r.MemberAssignment, assignment)
	return assignment, err
}

func (r *SyncGroupResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(r.Err))

	if err := pe.putBytes(r.MemberAssignment); err != nil {
		return err
	}

	return nil
}

func (r *SyncGroupResponse) decode(pd packetDecoder) (err error) {
	if kerr, err := pd.getInt16(); err != nil {
		return err
	} else {
		r.Err = KError(kerr)
	}

	maBytes, err := pd.getBytes()
	if err != nil {
		return err
	}

	r.MemberAssignment = maBytes

	return nil
}

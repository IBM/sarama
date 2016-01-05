package sarama

type SyncGroupResponse struct {
	Err              KError
	MemberAssignment *MemberAssignment
}

func (r *SyncGroupResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(r.Err))

	maBytes, err := encode(r.MemberAssignment)
	if err != nil {
		return err
	}

	if err := pe.putBytes(maBytes); err != nil {
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

	memberAssignment := new (MemberAssignment)
	if err := decode(maBytes, memberAssignment); err != nil {
		return err
	}

	r.MemberAssignment = memberAssignment

	return nil
}

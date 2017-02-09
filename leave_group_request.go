package sarama

type LeaveGroupRequest struct {
	GroupID  string
	MemberID string
}

func (r *LeaveGroupRequest) encode(pe packetEncoder) error {
	if err := pe.putString(r.GroupID); err != nil {
		return err
	}
	if err := pe.putString(r.MemberID); err != nil {
		return err
	}

	return nil
}

func (r *LeaveGroupRequest) decode(pd packetDecoder, version int16) (err error) {
	if r.GroupID, err = pd.getString(); err != nil {
		return
	}
	if r.MemberID, err = pd.getString(); err != nil {
		return
	}

	return nil
}

func (r *LeaveGroupRequest) key() int16 {
	return 13
}

func (r *LeaveGroupRequest) version() int16 {
	return 0
}

func (r *LeaveGroupRequest) requiredVersion() KafkaVersion {
	return V0_9_0_0
}

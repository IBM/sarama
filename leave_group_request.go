package sarama

//LeaveGroupRequest is a leave group request type
type LeaveGroupRequest struct {
	GroupID  string
	MemberID string
}

func (l *LeaveGroupRequest) encode(pe packetEncoder) error {
	if err := pe.putString(l.GroupID); err != nil {
		return err
	}
	if err := pe.putString(l.MemberID); err != nil {
		return err
	}

	return nil
}

func (l *LeaveGroupRequest) decode(pd packetDecoder, version int16) (err error) {
	if l.GroupID, err = pd.getString(); err != nil {
		return
	}
	if l.MemberID, err = pd.getString(); err != nil {
		return
	}

	return nil
}

func (l *LeaveGroupRequest) key() int16 {
	return 13
}

func (l *LeaveGroupRequest) version() int16 {
	return 0
}

func (l *LeaveGroupRequest) requiredVersion() KafkaVersion {
	return V0_9_0_0
}

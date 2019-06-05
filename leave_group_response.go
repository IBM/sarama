package sarama

//LeaveGroupResponse is a leave group response type
type LeaveGroupResponse struct {
	Err KError
}

func (l *LeaveGroupResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(l.Err))
	return nil
}

func (l *LeaveGroupResponse) decode(pd packetDecoder, version int16) (err error) {
	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}
	l.Err = KError(kerr)

	return nil
}

func (l *LeaveGroupResponse) key() int16 {
	return 13
}

func (l *LeaveGroupResponse) version() int16 {
	return 0
}

func (l *LeaveGroupResponse) requiredVersion() KafkaVersion {
	return V0_9_0_0
}

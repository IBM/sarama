package sarama

type HeartbeatRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
}

func (r *HeartbeatRequest) encode(pe packetEncoder) error {
	if err := pe.putString(r.GroupID); err != nil {
		return err
	}

	pe.putInt32(r.GenerationID)

	if err := pe.putString(r.MemberID); err != nil {
		return err
	}

	return nil
}

func (r *HeartbeatRequest) decode(pd packetDecoder, version int16) (err error) {
	if r.GroupID, err = pd.getString(); err != nil {
		return
	}
	if r.GenerationID, err = pd.getInt32(); err != nil {
		return
	}
	if r.MemberID, err = pd.getString(); err != nil {
		return
	}

	return nil
}

func (r *HeartbeatRequest) key() int16 {
	return 12
}

func (r *HeartbeatRequest) version() int16 {
	return 0
}

func (r *HeartbeatRequest) requiredVersion() KafkaVersion {
	return V0_9_0_0
}

package sarama

//HeartbeatRequest is a a heart beat request
type HeartbeatRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
}

func (h *HeartbeatRequest) encode(pe packetEncoder) error {
	if err := pe.putString(h.GroupID); err != nil {
		return err
	}

	pe.putInt32(h.GenerationID)

	if err := pe.putString(h.MemberID); err != nil {
		return err
	}

	return nil
}

func (h *HeartbeatRequest) decode(pd packetDecoder, version int16) (err error) {
	if h.GroupID, err = pd.getString(); err != nil {
		return
	}
	if h.GenerationID, err = pd.getInt32(); err != nil {
		return
	}
	if h.MemberID, err = pd.getString(); err != nil {
		return
	}

	return nil
}

func (h *HeartbeatRequest) key() int16 {
	return 12
}

func (h *HeartbeatRequest) version() int16 {
	return 0
}

func (h *HeartbeatRequest) requiredVersion() KafkaVersion {
	return V0_9_0_0
}

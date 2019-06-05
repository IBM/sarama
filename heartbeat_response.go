package sarama

//HeartbeatResponse is a heart beat response
type HeartbeatResponse struct {
	Err KError
}

func (h *HeartbeatResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(h.Err))
	return nil
}

func (h *HeartbeatResponse) decode(pd packetDecoder, version int16) error {
	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}
	h.Err = KError(kerr)

	return nil
}

func (h *HeartbeatResponse) key() int16 {
	return 12
}

func (h *HeartbeatResponse) version() int16 {
	return 0
}

func (h *HeartbeatResponse) requiredVersion() KafkaVersion {
	return V0_9_0_0
}

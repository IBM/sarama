package sarama

type HeartbeatRequest struct {
	Version         int16
	GroupId         string
	GenerationId    int32
	MemberId        string
	GroupInstanceId *string
}

func (r *HeartbeatRequest) setVersion(v int16) {
	r.Version = v
}

func (r *HeartbeatRequest) encode(pe packetEncoder) error {
	if err := pe.putString(r.GroupId); err != nil {
		return err
	}

	pe.putInt32(r.GenerationId)

	if err := pe.putString(r.MemberId); err != nil {
		return err
	}

	if r.Version >= 3 {
		if err := pe.putNullableString(r.GroupInstanceId); err != nil {
			return err
		}
	}

	return nil
}

func (r *HeartbeatRequest) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version
	if r.GroupId, err = pd.getString(); err != nil {
		return
	}
	if r.GenerationId, err = pd.getInt32(); err != nil {
		return
	}
	if r.MemberId, err = pd.getString(); err != nil {
		return
	}
	if r.Version >= 3 {
		if r.GroupInstanceId, err = pd.getNullableString(); err != nil {
			return
		}
	}

	return nil
}

func (r *HeartbeatRequest) key() int16 {
	return apiKeyHeartbeat
}

func (r *HeartbeatRequest) version() int16 {
	return r.Version
}

func (r *HeartbeatRequest) headerVersion() int16 {
	return 1
}

func (r *HeartbeatRequest) isValidVersion() bool {
	return r.Version >= 0 && r.Version <= 3
}

func (r *HeartbeatRequest) requiredVersion() KafkaVersion {
	switch r.Version {
	case 3:
		return V2_3_0_0
	case 2:
		return V2_0_0_0
	case 1:
		return V0_11_0_0
	case 0:
		return V0_8_2_0
	default:
		return V2_3_0_0
	}
}

package sarama

type SyncGroupResponse struct {
	Version          int16
	ThrottleTime     int32
	Err              KError
	MemberAssignment []byte
}

func (r *SyncGroupResponse) GetMemberAssignment() (*ConsumerGroupMemberAssignment, error) {
	assignment := new(ConsumerGroupMemberAssignment)
	err := decode(r.MemberAssignment, assignment)
	return assignment, err
}

func (r *SyncGroupResponse) encode(pe packetEncoder) error {
	if r.Version >= 1 {
		pe.putInt32(r.ThrottleTime)
	}
	pe.putInt16(int16(r.Err))
	return pe.putBytes(r.MemberAssignment)
}

func (r *SyncGroupResponse) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version
	if r.Version >= 1 {
		if r.ThrottleTime, err = pd.getInt32(); err != nil {
			return err
		}
	}
	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}

	r.Err = KError(kerr)

	r.MemberAssignment, err = pd.getBytes()
	return
}

func (r *SyncGroupResponse) key() int16 {
	return 14
}

func (r *SyncGroupResponse) version() int16 {
	return r.Version
}

func (r *SyncGroupResponse) headerVersion() int16 {
	return 0
}

func (r *SyncGroupResponse) requiredVersion() KafkaVersion {
	switch r.Version {
	case 1, 2, 3:
		return V2_3_0_0
	}
	return V0_9_0_0
}

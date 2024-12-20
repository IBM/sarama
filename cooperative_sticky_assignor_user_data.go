package sarama

type CooperativeStickyAssignorUserDataV0 struct {
	Generation int32
}

func (m *CooperativeStickyAssignorUserDataV0) encode(pe packetEncoder) error {
	pe.putInt32(m.Generation)
	return nil
}

func (m *CooperativeStickyAssignorUserDataV0) decode(pd packetDecoder) (err error) {
	if m.Generation, err = pd.getInt32(); err != nil {
		return
	}
	return nil
}

type CooperativeStickyMemberData struct {
	PartitionsAssignments []topicPartitionAssignment
	Generation            int32
	RackID                string
}

func (m *CooperativeStickyMemberData) partitions() []topicPartitionAssignment {
	return m.PartitionsAssignments
}
func (m *CooperativeStickyMemberData) hasGeneration() bool { return true }
func (m *CooperativeStickyMemberData) generation() int     { return int(m.Generation) }

var _ MemberData = (*CooperativeStickyMemberData)(nil)

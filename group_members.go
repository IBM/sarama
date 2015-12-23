package sarama

type GroupMemberMetadata struct {
	Version  int16
	Topics   []string
	UserData []byte
}

func (m *GroupMemberMetadata) encode(pe packetEncoder) error {
	pe.putInt16(m.Version)

	if err := pe.putStringArray(m.Topics); err != nil {
		return err
	}

	if err := pe.putBytes(m.UserData); err != nil {
		return err
	}

	return nil
}

func (m *GroupMemberMetadata) decode(pd packetDecoder) (err error) {
	if m.Version, err = pd.getInt16(); err != nil {
		return
	}

	if m.Topics, err = pd.getStringArray(); err != nil {
		return
	}

	if m.UserData, err = pd.getBytes(); err != nil {
		return
	}

	return nil
}

type GroupMemberAssignment struct {
	Version  int16
	Topics   []GroupMemberAssignedTopic
	UserData []byte
}

type GroupMemberAssignedTopic struct {
	Topic      string
	Partitions []int32
}

func (m *GroupMemberAssignment) encode(pe packetEncoder) error {
	pe.putInt16(m.Version)

	if err := pe.putArrayLength(len(m.Topics)); err != nil {
		return err
	}

	for _, topic := range m.Topics {
		if err := pe.putString(topic.Topic); err != nil {
			return err
		}
		if err := pe.putInt32Array(topic.Partitions); err != nil {
			return err
		}
	}

	if err := pe.putBytes(m.UserData); err != nil {
		return err
	}

	return nil
}

func (m *GroupMemberAssignment) decode(pd packetDecoder) (err error) {
	if m.Version, err = pd.getInt16(); err != nil {
		return
	}

	var topicLen int
	if topicLen, err = pd.getArrayLength(); err != nil {
		return
	}

	m.Topics = make([]GroupMemberAssignedTopic, topicLen)
	for i := 0; i < topicLen; i++ {
		if m.Topics[i].Topic, err = pd.getString(); err != nil {
			return
		}
		if m.Topics[i].Partitions, err = pd.getInt32Array(); err != nil {
			return
		}
	}

	if m.UserData, err = pd.getBytes(); err != nil {
		return
	}

	return nil
}

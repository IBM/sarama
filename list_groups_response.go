package sarama

//ListGroupsResponse lists a group response
type ListGroupsResponse struct {
	Err    KError
	Groups map[string]string
}

func (l *ListGroupsResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(l.Err))

	if err := pe.putArrayLength(len(l.Groups)); err != nil {
		return err
	}
	for groupID, protocolType := range l.Groups {
		if err := pe.putString(groupID); err != nil {
			return err
		}
		if err := pe.putString(protocolType); err != nil {
			return err
		}
	}

	return nil
}

func (l *ListGroupsResponse) decode(pd packetDecoder, version int16) error {
	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}

	l.Err = KError(kerr)

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	l.Groups = make(map[string]string)
	for i := 0; i < n; i++ {
		groupID, err := pd.getString()
		if err != nil {
			return err
		}
		protocolType, err := pd.getString()
		if err != nil {
			return err
		}

		l.Groups[groupID] = protocolType
	}

	return nil
}

func (l *ListGroupsResponse) key() int16 {
	return 16
}

func (l *ListGroupsResponse) version() int16 {
	return 0
}

func (l *ListGroupsResponse) requiredVersion() KafkaVersion {
	return V0_9_0_0
}

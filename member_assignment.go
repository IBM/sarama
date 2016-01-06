package sarama

type MemberAssignment struct {
	Version             int16
	PartitionAssignment map[string][]int32
	UserData            []byte
}

func (ma *MemberAssignment) encode(pe packetEncoder) error {
	pe.putInt16(int16(ma.Version))

	if err := pe.putArrayLength(len(ma.PartitionAssignment)); err != nil {
		return err
	}

	for topic, partitions := range ma.PartitionAssignment {
		if err := pe.putString(topic); err != nil {
			return err
		}

		if err := pe.putArrayLength(len(partitions)); err != nil {
			return err
		}

		for _, part := range partitions {
			pe.putInt32(int32(part))
		}
	}

	if err := pe.putBytes(ma.UserData); err != nil {
		return err
	}

	return nil
}

func (ma *MemberAssignment) decode(pd packetDecoder) error {
	if version, err := pd.getInt16(); err != nil {
		return err
	} else {
		ma.Version = version
	}

	ma.PartitionAssignment = make(map[string][]int32)
	if length, err := pd.getArrayLength(); err != nil {
		return err
	} else {
		for i := 0; i < length; i++ {
			topic, err := pd.getString()
			if err != nil {
				return err
			}

			n, err := pd.getArrayLength()
			if err != nil {
				return err
			}

			ma.PartitionAssignment[topic] = make([]int32, n)

			for pi := 0; pi < n; pi++ {
				if partition, err := pd.getInt32(); err != nil {
					return err
				} else {
					ma.PartitionAssignment[topic][pi] = partition
				}
			}
		}
	}

	if userData, err := pd.getBytes(); err != nil {
		return err
	} else {
		ma.UserData = userData
	}

	return nil
}

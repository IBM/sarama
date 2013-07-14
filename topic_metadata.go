package kafka

type TopicMetadata struct {
	Err        KError
	Name       *string
	Partitions []PartitionMetadata
}

func (tm *TopicMetadata) decode(pd packetDecoder) (err error) {
	tm.Err, err = pd.getError()
	if err != nil {
		return err
	}

	tm.Name, err = pd.getString()
	if err != nil {
		return err
	}

	n, err := pd.getArrayCount()
	if err != nil {
		return err
	}
	tm.Partitions = make([]PartitionMetadata, n)
	for i := 0; i < n; i++ {
		err = (&tm.Partitions[i]).decode(pd)
		if err != nil {
			return err
		}
	}

	return nil
}

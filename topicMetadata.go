package kafka

type topicMetadata struct {
	err        kError
	name       *string
	partitions []partitionMetadata
}

func (tm *topicMetadata) encode(pe packetEncoder) {
	pe.putError(tm.err)
	pe.putString(tm.name)
	pe.putArrayCount(len(tm.partitions))
	for i := range tm.partitions {
		(&tm.partitions[i]).encode(pe)
	}
}

func (tm *topicMetadata) decode(pd packetDecoder) (err error) {
	tm.err, err = pd.getError()
	if err != nil {
		return err
	}

	tm.name, err = pd.getString()
	if err != nil {
		return err
	}

	n, err := pd.getArrayCount()
	if err != nil {
		return err
	}
	tm.partitions = make([]partitionMetadata, n)
	for i := 0; i < n; i++ {
		err = (&tm.partitions[i]).decode(pd)
		if err != nil {
			return err
		}
	}

	return nil
}

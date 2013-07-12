package kafka

type produceResponsePartitionBlock struct {
	id     int32
	err    KError
	offset int64
}

func (pr *produceResponsePartitionBlock) decode(pd packetDecoder) (err error) {
	pr.id, err = pd.getInt32()
	if err != nil {
		return err
	}

	pr.err, err = pd.getError()
	if err != nil {
		return err
	}

	pr.offset, err = pd.getInt64()
	if err != nil {
		return err
	}

	return nil
}

type produceResponseTopicBlock struct {
	name       *string
	partitions []produceResponsePartitionBlock
}

func (pr *produceResponseTopicBlock) decode(pd packetDecoder) (err error) {
	pr.name, err = pd.getString()
	if err != nil {
		return err
	}

	n, err := pd.getArrayCount()
	if err != nil {
		return err
	}

	pr.partitions = make([]produceResponsePartitionBlock, n)
	for i := range pr.partitions {
		err = (&pr.partitions[i]).decode(pd)
		if err != nil {
			return err
		}
	}

	return nil
}

type produceResponse struct {
	topics []produceResponseTopicBlock
}

func (pr *produceResponse) decode(pd packetDecoder) (err error) {
	n, err := pd.getArrayCount()
	if err != nil {
		return err
	}

	pr.topics = make([]produceResponseTopicBlock, n)
	for i := range pr.topics {
		err = (&pr.topics[i]).decode(pd)
		if err != nil {
			return err
		}
	}

	return nil
}

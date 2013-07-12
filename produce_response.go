package kafka

type ProduceResponsePartitionBlock struct {
	Id     int32
	Err    KError
	Offset int64
}

func (pr *ProduceResponsePartitionBlock) decode(pd packetDecoder) (err error) {
	pr.Id, err = pd.getInt32()
	if err != nil {
		return err
	}

	pr.Err, err = pd.getError()
	if err != nil {
		return err
	}

	pr.Offset, err = pd.getInt64()
	if err != nil {
		return err
	}

	return nil
}

type ProduceResponseTopicBlock struct {
	Name       *string
	Partitions []ProduceResponsePartitionBlock
}

func (pr *ProduceResponseTopicBlock) decode(pd packetDecoder) (err error) {
	pr.Name, err = pd.getString()
	if err != nil {
		return err
	}

	n, err := pd.getArrayCount()
	if err != nil {
		return err
	}

	pr.Partitions = make([]ProduceResponsePartitionBlock, n)
	for i := range pr.Partitions {
		err = (&pr.Partitions[i]).decode(pd)
		if err != nil {
			return err
		}
	}

	return nil
}

type ProduceResponse struct {
	Topics []ProduceResponseTopicBlock
}

func (pr *ProduceResponse) decode(pd packetDecoder) (err error) {
	n, err := pd.getArrayCount()
	if err != nil {
		return err
	}

	pr.Topics = make([]ProduceResponseTopicBlock, n)
	for i := range pr.Topics {
		err = (&pr.Topics[i]).decode(pd)
		if err != nil {
			return err
		}
	}

	return nil
}

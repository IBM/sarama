package kafka

type ProduceResponsePartition struct {
	Err    KError
	Offset int64
}

func (pr *ProduceResponsePartition) decode(pd packetDecoder) (err error) {
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

type ProduceResponse struct {
	Partitions map[*string]map[int32]*ProduceResponsePartition
}

func (pr *ProduceResponse) decode(pd packetDecoder) (err error) {
	numTopics, err := pd.getArrayCount()
	if err != nil {
		return err
	}

	pr.Partitions = make(map[*string]map[int32]*ProduceResponsePartition, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.getString()
		if err != nil {
			return err
		}

		numPartitions, err := pd.getArrayCount()
		if err != nil {
			return err
		}

		pr.Partitions[name] = make(map[int32]*ProduceResponsePartition, numPartitions)

		for j := 0; j < numPartitions; j++ {
			id, err := pd.getInt32()
			if err != nil {
				return err
			}

			partition := new(ProduceResponsePartition)
			err = partition.decode(pd)
			if err != nil {
				return err
			}
			pr.Partitions[name][id] = partition
		}
	}

	return nil
}

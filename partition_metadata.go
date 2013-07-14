package kafka

type PartitionMetadata struct {
	Err      KError
	Id       int32
	Leader   int32
	Replicas []int32
	Isr      []int32
}

func (pm *PartitionMetadata) decode(pd packetDecoder) (err error) {
	pm.Err, err = pd.getError()
	if err != nil {
		return err
	}

	pm.Id, err = pd.getInt32()
	if err != nil {
		return err
	}

	pm.Leader, err = pd.getInt32()
	if err != nil {
		return err
	}

	pm.Replicas, err = pd.getInt32Array()
	if err != nil {
		return err
	}

	pm.Isr, err = pd.getInt32Array()
	if err != nil {
		return err
	}

	return nil
}

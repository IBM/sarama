package kafka

type partitionMetadata struct {
	err      KError
	id       int32
	leader   int32
	replicas []int32
	isr      []int32
}

func (pm *partitionMetadata) encode(pe packetEncoder) {
	pe.putError(pm.err)
	pe.putInt32(pm.id)
	pe.putInt32(pm.leader)
	pe.putInt32Array(pm.replicas)
	pe.putInt32Array(pm.isr)
}

func (pm *partitionMetadata) decode(pd packetDecoder) (err error) {
	pm.err, err = pd.getError()
	if err != nil {
		return err
	}

	pm.id, err = pd.getInt32()
	if err != nil {
		return err
	}

	pm.leader, err = pd.getInt32()
	if err != nil {
		return err
	}

	pm.replicas, err = pd.getInt32Array()
	if err != nil {
		return err
	}

	pm.isr, err = pd.getInt32Array()
	if err != nil {
		return err
	}

	return nil
}

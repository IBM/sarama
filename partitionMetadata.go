package kafka

type partitionMetadata struct {
	err      kError
	id       int32
	leader   int32
	replicas []int32
	isr      []int32
}

func (pm *partitionMetadata) encode(pe packetEncoder) {
	pe.putError(pm.err)
	pe.putInt32(pm.id)
	pe.putInt32(pm.leader)

	pe.putArrayCount(len(pm.replicas))
	for _, val := range pm.replicas {
		pe.putInt32(val)
	}

	pe.putArrayCount(len(pm.isr))
	for _, val := range pm.isr {
		pe.putInt32(val)
	}
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

	n, err := pd.getArrayCount()
	if err != nil {
		return err
	}
	pm.replicas = make([]int32, n)
	for i := 0; i < n; i++ {
		pm.replicas[i], err = pd.getInt32()
		if err != nil {
			return err
		}
	}

	n, err = pd.getArrayCount()
	if err != nil {
		return err
	}
	pm.isr = make([]int32, n)
	for i := 0; i < n; i++ {
		pm.isr[i], err = pd.getInt32()
		if err != nil {
			return err
		}
	}

	return nil
}

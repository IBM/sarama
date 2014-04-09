package sarama

type OffsetCommitResponse struct {
	Errors map[string]map[int32]KError
}

func (r *OffsetCommitResponse) decode(pd packetDecoder) (err error) {
	numTopics, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	r.Errors = make(map[string]map[int32]KError, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.getString()
		if err != nil {
			return err
		}

		numErrors, err := pd.getArrayLength()
		if err != nil {
			return err
		}

		r.Errors[name] = make(map[int32]KError, numErrors)

		for j := 0; j < numErrors; j++ {
			id, err := pd.getInt32()
			if err != nil {
				return err
			}

			tmp, err := pd.getInt16()
			if err != nil {
				return err
			}
			r.Errors[name][id] = KError(tmp)
		}
	}

	return nil
}

package kafka

type metadataRequest struct {
	topics []*string
}

func (mr *metadataRequest) encode(pe packetEncoder) {
	pe.putArrayCount(len(mr.topics))
	for i := range mr.topics {
		pe.putString(mr.topics[i])
	}
}

func (mr *metadataRequest) decode(pd packetDecoder) (err error) {
	n, err := pd.getArrayCount()
	if err != nil {
		return err
	}

	mr.topics = make([]*string, n)
	for i := 0; i < n; i++ {
		mr.topics[i], err = pd.getString()
		if err != nil {
			return err
		}
	}

	return nil
}

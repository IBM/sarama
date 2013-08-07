package sarama

type MetadataRequest struct {
	Topics []string
}

func (mr *MetadataRequest) encode(pe packetEncoder) error {
	err := pe.putArrayLength(len(mr.Topics))
	if err != nil {
		return err
	}

	for i := range mr.Topics {
		err = pe.putString(mr.Topics[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (mr *MetadataRequest) key() int16 {
	return 3
}

func (mr *MetadataRequest) version() int16 {
	return 0
}

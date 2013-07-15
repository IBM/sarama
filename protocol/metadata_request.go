package protocol

type MetadataRequest struct {
	Topics []string
}

func (mr *MetadataRequest) encode(pe packetEncoder) {
	pe.putArrayCount(len(mr.Topics))
	for i := range mr.Topics {
		pe.putString(mr.Topics[i])
	}
}

func (mr *MetadataRequest) key() int16 {
	return 3
}

func (mr *MetadataRequest) version() int16 {
	return 0
}

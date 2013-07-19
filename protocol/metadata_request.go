package protocol

import enc "sarama/encoding"

type MetadataRequest struct {
	Topics []string
}

func (mr *MetadataRequest) Encode(pe enc.PacketEncoder) error {
	err := pe.PutArrayLength(len(mr.Topics))
	if err != nil {
		return err
	}

	for i := range mr.Topics {
		err = pe.PutString(mr.Topics[i])
		if err != nil {
			return err
		}
	}
}

func (mr *MetadataRequest) key() int16 {
	return 3
}

func (mr *MetadataRequest) version() int16 {
	return 0
}

package kafka

type topicMetadata struct {
	err        kafkaError
	name       *string
	partitions []partitionMetadata
}

func (tm *topicMetadata) length() (int, error) {
	length := 2
	length, err := stringLength(tm.name)
	if err != nil {
		return -1, err
	}
	length += 4
	for i := range tm.partitions {
		tmp, err := (&tm.partitions[i]).length()
		if err != nil {
			return -1, err
		}
		length += tmp
	}
	return length, nil
}

func (tm *topicMetadata) encode(buf []byte, off int) int {
	off = encodeError(buf, off, tm.err)
	off = encodeString(buf, off, tm.name)
	off = encodeInt32(buf, off, int32(len(tm.partitions)))
	for i := range tm.partitions {
		off = (&tm.partitions[i]).encode(buf, off)
	}
	return off
}

func (tm *topicMetadata) decode(buf []byte, off int) (int, error) {
}

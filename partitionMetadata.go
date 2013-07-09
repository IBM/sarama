package kafka

import "errors"

type partitionMetadata struct {
	err      kafkaError
	id       int32
	leader   int32
	replicas []int32
	isr      []int32
}

func (pm *partitionMetadata) length() (int, error) {
	length := 6

	length += 4
	length += len(pm.replicas) * 4

	length += 4
	length += len(pm.isr) * 4

	return length, nil
}

func (pm *partitionMetadata) encode(buf []byte, off int) int {
	off = encodeError(buf, off, pm.err)
	off = encodeInt32(buf, off, pm.id)
	off = encodeInt32(buf, off, pm.leader)

	off = encodeInt32(buf, off, int32(len(pm.replicas)))
	for _, val := range pm.replicas {
		off = encodeInt32(buf, off, val)
	}

	off = encodeInt32(buf, off, int32(len(pm.isr)))
	for _, val := range pm.isr {
		off = encodeInt32(buf, off, val)
	}

	return off
}

func (pm *partitionMetadata) decode(buf []byte, off int) (int, error) {
	if len(buf)-off < 14 {
		return -1, errors.New("kafka decode: not enough data")
	}

	pm.err, off = decodeError(buf, off)
	pm.id, off = decodeInt32(buf, off)
	pm.leader, off = decodeInt32(buf, off)

	tmp, off := decodeInt32(buf, off)
	length := int(tmp)
	if length > (len(buf)-off)/4 {
		return -1, errors.New("kafka decode: not enough data")
	}
	pm.replicas = make([]int32, length)
	for i := 0; i < length; i++ {
		pm.replicas[i], off = decodeInt32(buf, off)
	}

	tmp, off = decodeInt32(buf, off)
	length = int(tmp)
	if length > (len(buf)-off)/4 {
		return -1, errors.New("kafka decode: not enough data")
	}
	pm.isr = make([]int32, length)
	for i := 0; i < length; i++ {
		pm.isr[i], off = decodeInt32(buf, off)
	}

	return off, nil
}

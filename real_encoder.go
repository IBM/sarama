package sarama

import "encoding/binary"

type realEncoder struct {
	raw   []byte
	off   int
	stack []pushEncoder

	// For exporting statistics
	currentTopic             string
	batchSizeObserver        func(string, int64)
	recordCountObserver      func(string, int64)
	compressionRatioObserver func(string, float64)
}

// primitives

func (re *realEncoder) putInt8(in int8) {
	re.raw[re.off] = byte(in)
	re.off++
}

func (re *realEncoder) putInt16(in int16) {
	binary.BigEndian.PutUint16(re.raw[re.off:], uint16(in))
	re.off += 2
}

func (re *realEncoder) putInt32(in int32) {
	binary.BigEndian.PutUint32(re.raw[re.off:], uint32(in))
	re.off += 4
}

func (re *realEncoder) putInt64(in int64) {
	binary.BigEndian.PutUint64(re.raw[re.off:], uint64(in))
	re.off += 8
}

func (re *realEncoder) putArrayLength(in int) error {
	re.putInt32(int32(in))
	return nil
}

// collection

func (re *realEncoder) putRawBytes(in []byte) error {
	copy(re.raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *realEncoder) putBytes(in []byte) error {
	if in == nil {
		re.putInt32(-1)
		return nil
	}
	re.putInt32(int32(len(in)))
	copy(re.raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *realEncoder) putString(in string) error {
	re.putInt16(int16(len(in)))
	copy(re.raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *realEncoder) putStringArray(in []string) error {
	err := re.putArrayLength(len(in))
	if err != nil {
		return err
	}

	for _, val := range in {
		if err := re.putString(val); err != nil {
			return err
		}
	}

	return nil
}

func (re *realEncoder) putInt32Array(in []int32) error {
	err := re.putArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		re.putInt32(val)
	}
	return nil
}

func (re *realEncoder) putInt64Array(in []int64) error {
	err := re.putArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		re.putInt64(val)
	}
	return nil
}

// stacks

func (re *realEncoder) push(in pushEncoder) {
	in.saveOffset(re.off)
	re.off += in.reserveLength()
	re.stack = append(re.stack, in)
}

func (re *realEncoder) pop() error {
	// this is go's ugly pop pattern (the inverse of append)
	in := re.stack[len(re.stack)-1]
	re.stack = re.stack[:len(re.stack)-1]

	return in.run(re.off, re.raw)
}

func (re *realEncoder) offset() int {
	return re.off
}

func (re *realEncoder) recordTopic(topic string) {
	re.currentTopic = topic
}

func (re *realEncoder) recordBatchSize(size int) {
	if re.batchSizeObserver != nil {
		re.batchSizeObserver(re.currentTopic, int64(size))
	}
}

func (re *realEncoder) recordRecordCount(count int) {
	if re.recordCountObserver != nil {
		re.recordCountObserver(re.currentTopic, int64(count))
	}
}

func (re *realEncoder) recordCompressionRatio(ratio float64) {
	if re.compressionRatioObserver != nil {
		re.compressionRatioObserver(re.currentTopic, ratio)
	}
}

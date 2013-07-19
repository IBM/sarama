package encoding

import "encoding/binary"

type realEncoder struct {
	raw   []byte
	off   int
	stack []PushEncoder
}

// primitives

func (re *realEncoder) PutInt8(in int8) error {
	re.raw[re.off] = byte(in)
	re.off += 1
	return nil
}

func (re *realEncoder) PutInt16(in int16) error {
	binary.BigEndian.PutUint16(re.raw[re.off:], uint16(in))
	re.off += 2
	return nil
}

func (re *realEncoder) PutInt32(in int32) error {
	binary.BigEndian.PutUint32(re.raw[re.off:], uint32(in))
	re.off += 4
	return nil
}

func (re *realEncoder) PutInt64(in int64) error {
	binary.BigEndian.PutUint64(re.raw[re.off:], uint64(in))
	re.off += 8
	return nil
}

// collection

func (re *realEncoder) PutBytes(in []byte) error {
	if in == nil {
		re.PutInt32(-1)
		return nil
	}
	re.PutInt32(int32(len(in)))
	copy(re.raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *realEncoder) PutString(in string) error {
	re.PutInt16(int16(len(in)))
	copy(re.raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *realEncoder) PutInt32Array(in []int32) error {
	re.PutInt32(int32(len(in)))
	for _, val := range in {
		re.PutInt32(val)
	}
	return nil
}

// stacks

func (re *realEncoder) Push(in PushEncoder) error {
	in.SaveOffset(re.off)
	re.off += in.ReserveLength()
	re.stack = append(re.stack, in)
	return nil
}

func (re *realEncoder) Pop() error {
	// this is go's ugly pop pattern (the inverse of append)
	in := re.stack[len(re.stack)-1]
	re.stack = re.stack[:len(re.stack)-1]

	return in.Run(re.off, re.raw)
}

package encoding

import "math"

type prepEncoder struct {
	length int
}

// primitives

func (pe *prepEncoder) PutInt8(in int8) error {
	pe.length += 1
	return nil
}

func (pe *prepEncoder) PutInt16(in int16) error {
	pe.length += 2
	return nil
}

func (pe *prepEncoder) PutInt32(in int32) error {
	pe.length += 4
	return nil
}

func (pe *prepEncoder) PutInt64(in int64) error {
	pe.length += 8
	return nil
}

// arrays

func (pe *prepEncoder) PutBytes(in []byte) error {
	pe.length += 4
	if in == nil {
		return nil
	}
	if len(in) > math.MaxInt32 {
		return EncodingError
	}
	pe.length += len(in)
	return nil
}

func (pe *prepEncoder) PutString(in string) error {
	pe.length += 2
	if len(in) > math.MaxInt16 {
		return EncodingError
	}
	pe.length += len(in)
	return nil
}

func (pe *prepEncoder) PutInt32Array(in []int32) error {
	pe.length += 4
	pe.length += 4 * len(in)
	return nil
}

// stackable

func (pe *prepEncoder) Push(in PushEncoder) error {
	pe.length += in.ReserveLength()
	return nil
}

func (pe *prepEncoder) Pop() error {
	return nil
}

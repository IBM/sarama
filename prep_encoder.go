package kafka

import "math"

type prepEncoder struct {
	length int
	err    error
}

// primitives

func (pe *prepEncoder) putInt8(in int8) {
	pe.length += 1
}

func (pe *prepEncoder) putInt16(in int16) {
	pe.length += 2
}

func (pe *prepEncoder) putInt32(in int32) {
	pe.length += 4
}

func (pe *prepEncoder) putInt64(in int64) {
	pe.length += 8
}

// arrays

func (pe *prepEncoder) putInt32Array(in []int32) {
	pe.length += 4
	pe.length += 4 * len(in)
}

func (pe *prepEncoder) putArrayCount(in int) {
	pe.length += 4
}

// misc

func (pe *prepEncoder) putError(in KError) {
	pe.length += 2
}

func (pe *prepEncoder) putString(in *string) {
	pe.length += 2
	if in == nil {
		return
	}
	if len(*in) > math.MaxInt16 {
		pe.err = EncodingError{"String too long"}
	} else {
		pe.length += len(*in)
	}
}

func (pe *prepEncoder) putBytes(in *[]byte) {
	pe.length += 4
	if in == nil {
		return
	}
	if len(*in) > math.MaxInt32 {
		pe.err = EncodingError{"Bytes too long"}
	} else {
		pe.length += len(*in)
	}
}

func (pe *prepEncoder) putRaw(in []byte) {
	pe.length += len(in)
}

// stackable

func (pe *prepEncoder) push(in pushEncoder) {
	pe.length += in.reserveLength()
}

func (pe *prepEncoder) pushLength32() {
	pe.length += 4
}

func (pe *prepEncoder) pushCRC32() {
	pe.length += 4
}

func (pe *prepEncoder) pop() {
}

package kafka

import "math"

type prepEncoder struct {
	length int
	err    bool
}

func (pe *prepEncoder) putInt16(in int16) {
	pe.length += 2
}

func (pe *prepEncoder) putInt32(in int32) {
	pe.length += 4
}

func (pe *prepEncoder) putError(in kError) {
	pe.length += 2
}

func (pe *prepEncoder) putString(in *string) {
	pe.length += 2
	if in == nil {
		return
	}
	if len(*in) > math.MaxInt16 {
		pe.err = true
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
		pe.err = true
	} else {
		pe.length += len(*in)
	}
}

func (pe *prepEncoder) putArrayCount(in int) {
	pe.length += 4
}

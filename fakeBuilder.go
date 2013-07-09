package kafka

import "math"

type fakeBuilder struct {
	length int
	err    bool
}

func (fb *fakeBuilder) putInt16(in int16) {
	fb.length += 2
}

func (fb *fakeBuilder) putInt32(in int32) {
	fb.length += 4
}

func (fb *fakeBuilder) putError(in kafkaError) {
	fb.length += 2
}

func (fb *fakeBuilder) putString(in *string) {
	fb.length += 2
	if in == nil {
		return
	}
	if len(*in) > math.MaxInt16 {
		fb.err = true
	} else {
		fb.length += len(*in)
	}
}

func (fb *fakeBuilder) putBytes(in *[]byte) {
	fb.length += 4
	if in == nil {
		return
	}
	if len(*in) > math.MaxInt32 {
		fb.err = true
	} else {
		fb.length += len(*in)
	}
}

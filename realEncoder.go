package kafka

import "encoding/binary"

type realEncoder struct {
	raw []byte
	off int
}

func (re *realEncoder) putInt16(in int16) {
	binary.BigEndian.PutUint16(re.raw[re.off:], uint16(in))
	re.off += 2
}

func (re *realEncoder) putInt32(in int32) {
	binary.BigEndian.PutUint32(re.raw[re.off:], uint32(in))
	re.off += 4
}

func (re *realEncoder) putError(in KError) {
	re.putInt16(int16(in))
}

func (re *realEncoder) putString(in *string) {
	if in == nil {
		re.putInt16(-1)
		return
	}
	re.putInt16(int16(len(*in)))
	re.off += 2
	copy(re.raw[re.off:], *in)
	re.off += len(*in)
}

func (re *realEncoder) putBytes(in *[]byte) {
	if in == nil {
		re.putInt32(-1)
		return
	}
	re.putInt32(int32(len(*in)))
	re.off += 4
	copy(re.raw[re.off:], *in)
	re.off += len(*in)
}

func (re *realEncoder) putArrayCount(in int) {
	re.putInt32(int32(in))
}

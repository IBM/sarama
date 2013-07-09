package kafka

import "encoding/binary"

type realBuilder struct {
	raw []byte
	off int
}

func (rb *realBuilder) putInt16(in int16) {
	binary.BigEndian.PutUint16(b.raw[b.off:], uint16(in))
	rb.off += 2
}

func (rb *realBuilder) putInt32(in int32) {
	binary.BigEndian.PutUint32(b.raw[b.off:], uint32(in))
	rb.off += 4
}

func (rb *realBuilder) putError(in kafkaError) {
	rb.putInt16(int16(in))
}

func (rb *realBuilder) putString(in *string) {
	if in == nil {
		rb.putInt16(-1)
		return
	}
	rb.putInt16(int16(len(*in)))
	rb.off += 2
	copy(rb.raw[off:], *in)
	rb.off += len(*in)
}

func (rb *realBuilder) putBytes(in *[]byte) {
	if in == nil {
		rb.putInt32(-1)
		return
	}
	rb.putInt32(int32(len(*in)))
	rb.off += 4
	copy(rb.raw[off:], *in)
	rb.off += len(*in)
}

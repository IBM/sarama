package kafka

import "encoding/binary"

type realEncoder struct {
	raw   []byte
	off   int
	stack []pushEncoder
}

func (re *realEncoder) putInt8(in int8) {
	re.raw[re.off] = byte(in)
	re.off += 1
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

func (re *realEncoder) push(in pushEncoder) {
	in.saveOffset(re.off)
	re.off += in.reserveLength()
	re.stack = append(re.stack, in)
}

func (re *realEncoder) pushLength32() {
	re.push(&length32Encoder{})
}

func (re *realEncoder) pushCRC32() {
	re.push(&crc32Encoder{})
}

func (re *realEncoder) pop() {
	// this is go's ugly pop pattern (the inverse of append)
	in := re.stack[len(re.stack)-1]
	re.stack = re.stack[:len(re.stack)-1]

	in.run(re.off, re.raw)
}

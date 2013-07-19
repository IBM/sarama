package encoding

type packetEncoder interface {
	// primitives
	putInt8(in int8)
	putInt16(in int16)
	putInt32(in int32)
	putInt64(in int64)

	// arrays
	putInt32Array(in []int32)
	putArrayCount(in int)

	// misc
	putString(in string)
	putBytes(in []byte)
	putRaw(in []byte)

	// stackable
	push(in pushEncoder)
	pushLength32()
	pushCRC32()
	pop()
}

type pushEncoder interface {
	saveOffset(in int)
	reserveLength() int
	run(curOffset int, buf []byte)
}

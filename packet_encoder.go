package kafka

type packetEncoder interface {
	putInt8(in int8)
	putInt16(in int16)
	putInt32(in int32)
	putInt64(in int64)

	putError(in KError)
	putString(in *string)
	putBytes(in *[]byte)
	putArrayCount(in int)

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

package kafka

type packetDecoder interface {
	remaining() int

	getInt16() (int16, error)
	getInt32() (int32, error)
	getInt64() (int64, error)

	getError() (KError, error)
	getString() (*string, error)
	getBytes() (*[]byte, error)
	getArrayCount() (int, error)

	push(in pushDecoder) error
	pushLength32() error
	pushCRC32() error
	pop() error
}

type pushDecoder interface {
	saveOffset(in int)
	reserveLength() int
	check(curOffset int, buf []byte) error
}

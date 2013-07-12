package kafka

type packetDecoder interface {
	remaining() int

	// primitives
	getInt8() (int8, error)
	getInt16() (int16, error)
	getInt32() (int32, error)
	getInt64() (int64, error)

	// arrays
	getInt32Array() ([]int32, error)
	getArrayCount() (int, error)

	// misc
	getError() (KError, error)
	getString() (*string, error)
	getBytes() (*[]byte, error)

	// stackable
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

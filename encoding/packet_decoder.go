package encoding

// PacketDecoder is the interface providing helpers for reading with Kafka's encoding rules.
// Types implementing Decoder only need to worry about calling methods like GetString,
// not about how a string is represented in Kafka.
type PacketDecoder interface {
	// Primitives
	GetInt8() (int8, error)
	GetInt16() (int16, error)
	GetInt32() (int32, error)
	GetInt64() (int64, error)
	GetArrayLength() (int, error)

	// Collections
	GetBytes() ([]byte, error)
	GetString() (string, error)
	GetInt32Array() ([]int32, error)
	GetInt64Array() ([]int64, error)

	// Subsets
	Remaining() int
	GetSubset(length int) (PacketDecoder, error)

	// Stacks, see PushDecoder
	Push(in PushDecoder) error
	Pop() error
}

// PushDecoder is the interface for decoding fields like CRCs and lengths where the validity
// of the field depends on what is after it in the packet. Start them with PacketDecoder.Push() where
// the actual value is located in the packet, then PacketDecoder.Pop() them when all the bytes they
// depend upon have been decoded.
type PushDecoder interface {
	// Saves the offset into the input buffer as the location to actually read the calculated value when able.
	SaveOffset(in int)

	// Returns the length of data to reserve for the input of this encoder (eg 4 bytes for a CRC32).
	ReserveLength() int

	// Indicates that all required data is now available to calculate and check the field.
	// SaveOffset is guaranteed to have been called first. The implementation should read ReserveLength() bytes
	// of data from the saved offset, and verify it based on the data between the saved offset and curOffset.
	Check(curOffset int, buf []byte) error
}

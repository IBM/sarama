package encoding

// PacketEncoder is the interface providing helpers for writing with Kafka's encoding rules.
// Types implementing Encoder only need to worry about calling methods like PutString,
// not about how a string is represented in Kafka.
type PacketEncoder interface {
	// Primitives
	PutInt8(in int8)
	PutInt16(in int16)
	PutInt32(in int32)
	PutInt64(in int64)
	PutArrayLength(in int) error

	// Collections
	PutBytes(in []byte) error
	PutString(in string) error
	PutInt32Array(in []int32) error

	// Stacks, see PushEncoder
	Push(in PushEncoder)
	Pop() error
}

// PushEncoder is the interface for encoding fields like CRCs and lengths where the value
// of the field depends on what is encoded after it in the packet. Start them with PacketEncoder.Push() where
// the actual value is located in the packet, then PacketEncoder.Pop() them when all the bytes they
// depend upon have been written.
type PushEncoder interface {
	// Saves the offset into the input buffer as the location to actually write the calculated value when able.
	SaveOffset(in int)

	// Returns the length of data to reserve for the output of this encoder (eg 4 bytes for a CRC32).
	ReserveLength() int

	// Indicates that all required data is now available to calculate and write the field.
	// SaveOffset is guaranteed to have been called first. The implementation should write ReserveLength() bytes
	// of data to the saved offset, based on the data between the saved offset and curOffset.
	Run(curOffset int, buf []byte) error
}

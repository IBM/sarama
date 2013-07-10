package kafka

type packetDecoder interface {
	getInt16() (int16, error)
	getInt32() (int32, error)
	getError() (kafkaError, error)
	getString() (*string, error)
	getBytes() (*[]byte, error)
	getArrayCount() (int, error)
}

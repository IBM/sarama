package kafka

type encoder interface {
	encode(pe packetEncoder)
}

type decoder interface {
	decode(pd packetDecoder) error
}

type encoderDecoder interface {
	encoder
	decoder
}

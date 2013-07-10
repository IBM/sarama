package kafka

type encoder interface {
	encode(pe packetEncoder)
}

type decoder interface {
	decoder(pd packetDecoder)
}

type encoderDecoder interface {
	encoder
	decoder
}

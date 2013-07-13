package kafka

import "encoding/binary"

type length32Encoder struct {
	startOffset int
}

func (l *length32Encoder) saveOffset(in int) {
	l.startOffset = in
}

func (l *length32Encoder) reserveLength() int {
	return 4
}

func (l *length32Encoder) run(curOffset int, buf []byte) {
	binary.BigEndian.PutUint32(buf[l.startOffset:], uint32(curOffset-l.startOffset-4))
}

type length32Decoder struct {
	startOffset int
}

func (l *length32Decoder) saveOffset(in int) {
	l.startOffset = in
}

func (l *length32Decoder) reserveLength() int {
	return 4
}

func (l *length32Decoder) check(curOffset int, buf []byte) error {
	if uint32(curOffset-l.startOffset-4) != binary.BigEndian.Uint32(buf[l.startOffset:]) {
		return DecodingError("Packet length did not match.")
	}

	return nil
}

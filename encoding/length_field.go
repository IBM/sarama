package encoding

import "encoding/binary"

// LengthField implements the PushEncoder and PushDecoder interfaces for calculating 4-byte lengths.
type LengthField struct {
	startOffset int
}

func (l *LengthField) SaveOffset(in int) {
	l.startOffset = in
}

func (l *LengthField) ReserveLength() int {
	return 4
}

func (l *LengthField) Run(curOffset int, buf []byte) error {
	binary.BigEndian.PutUint32(buf[l.startOffset:], uint32(curOffset-l.startOffset-4))
	return nil
}

func (l *LengthField) Check(curOffset int, buf []byte) error {
	if uint32(curOffset-l.startOffset-4) != binary.BigEndian.Uint32(buf[l.startOffset:]) {
		return DecodingError
	}

	return nil
}

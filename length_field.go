package sarama

import "encoding/binary"

// LengthField implements the PushEncoder and PushDecoder interfaces for calculating 4-byte lengths.
type lengthField struct {
	startOffset int
}

func (l *lengthField) saveOffset(in int) {
	l.startOffset = in
}

func (l *lengthField) reserveLength() int {
	return 4
}

func (l *lengthField) run(curOffset int, buf []byte) error {
	binary.BigEndian.PutUint32(buf[l.startOffset:], uint32(curOffset-l.startOffset-4))
	return nil
}

func (l *lengthField) check(curOffset int, buf []byte) error {
	if uint32(curOffset-l.startOffset-4) != binary.BigEndian.Uint32(buf[l.startOffset:]) {
		return PacketDecodingError{"length field invalid"}
	}

	return nil
}

type varintLengthField struct {
	startOffset int
	length      int64
	adjusted    bool
	size        int
}

func (l *varintLengthField) decode(pd packetDecoder) error {
	var err error
	l.length, err = pd.getVarint()
	return err
}

func (l *varintLengthField) saveOffset(in int) {
	l.startOffset = in
}

func (l *varintLengthField) adjustLength(currOffset int) int {
	l.adjusted = true

	var tmp [binary.MaxVarintLen64]byte
	l.length = int64(currOffset - l.startOffset - l.size)

	newSize := binary.PutVarint(tmp[:], l.length)
	diff := newSize - l.size
	l.size = newSize

	return diff
}

func (l *varintLengthField) reserveLength() int {
	return l.size
}

func (l *varintLengthField) run(curOffset int, buf []byte) error {
	if !l.adjusted {
		return PacketEncodingError{"varintLengthField.run called before adjustLength"}
	}
	binary.PutVarint(buf[l.startOffset:], l.length)
	return nil
}

func (l *varintLengthField) check(curOffset int, buf []byte) error {
	if int64(curOffset-l.startOffset) != l.length {
		return PacketDecodingError{"length field invalid"}
	}

	return nil
}

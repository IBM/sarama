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
}

func newVarintLengthField(pd packetDecoder) (*varintLengthField, error) {
	n, err := pd.getVarint()
	if err != nil {
		return nil, err
	}
	return &varintLengthField{length: n}, nil
}

func (l *varintLengthField) saveOffset(in int) {
	l.startOffset = in
}

func (l *varintLengthField) reserveLength() int {
	return 0
}

func (l *varintLengthField) check(curOffset int, buf []byte) error {
	if int64(curOffset-l.startOffset) != l.length {
		return PacketDecodingError{"length field invalid"}
	}

	return nil
}

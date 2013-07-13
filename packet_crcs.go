package kafka

import (
	"encoding/binary"
	"hash/crc32"
)

type crc32Encoder struct {
	startOffset int
}

func (c *crc32Encoder) saveOffset(in int) {
	c.startOffset = in
}

func (c *crc32Encoder) reserveLength() int {
	return 4
}

func (c *crc32Encoder) run(curOffset int, buf []byte) {
	crc := crc32.ChecksumIEEE(buf[c.startOffset+4 : curOffset])
	binary.BigEndian.PutUint32(buf[c.startOffset:], crc)
}

type crc32Decoder struct {
	startOffset int
}

func (c *crc32Decoder) saveOffset(in int) {
	c.startOffset = in
}

func (c *crc32Decoder) reserveLength() int {
	return 4
}

func (c *crc32Decoder) check(curOffset int, buf []byte) error {
	crc := crc32.ChecksumIEEE(buf[c.startOffset+4 : curOffset])

	if crc != binary.BigEndian.Uint32(buf[c.startOffset:]) {
		return DecodingError("CRC did not match.")
	}

	return nil
}

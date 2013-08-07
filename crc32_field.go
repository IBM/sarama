package kafka

import (
	"encoding/binary"
	"hash/crc32"
)

// crc32field implements the pushEncoder and pushDecoder interfaces for calculating CRC32s.
type crc32field struct {
	startOffset int
}

func (c *crc32field) saveOffset(in int) {
	c.startOffset = in
}

func (c *crc32field) reserveLength() int {
	return 4
}

func (c *crc32field) run(curOffset int, buf []byte) error {
	crc := crc32.ChecksumIEEE(buf[c.startOffset+4 : curOffset])
	binary.BigEndian.PutUint32(buf[c.startOffset:], crc)
	return nil
}

func (c *crc32field) check(curOffset int, buf []byte) error {
	crc := crc32.ChecksumIEEE(buf[c.startOffset+4 : curOffset])

	if crc != binary.BigEndian.Uint32(buf[c.startOffset:]) {
		return DecodingError
	}

	return nil
}

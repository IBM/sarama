package encoding

import (
	"encoding/binary"
	"hash/crc32"
)

// CRC32Field implements the PushEncoder and PushDecoder interfaces for calculating CRC32s.
type CRC32Field struct {
	startOffset int
}

func (c *CRC32Field) SaveOffset(in int) {
	c.startOffset = in
}

func (c *CRC32Field) ReserveLength() int {
	return 4
}

func (c *CRC32Field) Run(curOffset int, buf []byte) error {
	crc := crc32.ChecksumIEEE(buf[c.startOffset+4 : curOffset])
	binary.BigEndian.PutUint32(buf[c.startOffset:], crc)
	return nil
}

func (c *CRC32Field) Check(curOffset int, buf []byte) error {
	crc := crc32.ChecksumIEEE(buf[c.startOffset+4 : curOffset])

	if crc != binary.BigEndian.Uint32(buf[c.startOffset:]) {
		return DecodingError
	}

	return nil
}

package sarama

import (
	"bytes"
	"code.google.com/p/snappy-go/snappy"
	"encoding/binary"
	_ "fmt"
)

var snappyMagic = []byte{130, 83, 78, 65, 80, 80, 89, 0}

// SnappyEncode encodes binary data
func SnappyEncode(src []byte) ([]byte, error) {
	return snappy.Encode(nil, src)
}

// SnappyDecode decodes snappy data
func SnappyDecode(src []byte) ([]byte, error) {
	if bytes.Equal(src[:8], snappyMagic) {
		pos := uint32(16)
		max := uint32(len(src))
		dst := make([]byte, 0)
		for pos < max {
			size := binary.BigEndian.Uint32(src[pos : pos+4])
			pos = pos + 4
			chunk, err := snappy.Decode(nil, src[pos:pos+size])
			if err != nil {
				return nil, err
			}
			pos = pos + size
			dst = append(dst, chunk...)
		}
		return dst, nil
	}
	return snappy.Decode(nil, src)
}

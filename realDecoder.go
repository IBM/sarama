package kafka

import (
	"encoding/binary"
	"math"
)

type realDecoder struct {
	raw []byte
	off int
}

func (rd *realDecoder) avail() int {
	return len(rd.raw) - rd.off
}

func (rd *realDecoder) getInt16() (int16, error) {
	if rd.avail() < 2 {
		return -1, decodingError{}
	}
	tmp := int16(binary.BigEndian.Uint16(rd.raw[rd.off:]))
	rd.off += 2
	return tmp, nil
}

func (rd *realDecoder) getInt32() (int32, error) {
	if rd.avail() < 4 {
		return -1, decodingError{}
	}
	tmp := int32(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4
	return tmp, nil
}

func (rd *realDecoder) getError() (kError, error) {
	val, err := rd.getInt16()
	return kError(val), err
}

func (rd *realDecoder) getString() (*string, error) {
	tmp, err := rd.getInt16()

	if err != nil {
		return nil, err
	}

	n := int(tmp)

	switch {
	case n < -1:
		return nil, decodingError{}
	case n == -1:
		return nil, nil
	case n == 0:
		return new(string), nil
	case n > rd.avail():
		return nil, decodingError{}
	default:
		tmp := new(string)
		*tmp = string(rd.raw[rd.off : rd.off+n])
		return tmp, nil
	}
}

func (rd *realDecoder) getBytes() (*[]byte, error) {
	tmp, err := rd.getInt32()

	if err != nil {
		return nil, err
	}

	n := int(tmp)

	switch {
	case n < -1:
		return nil, decodingError{}
	case n == -1:
		return nil, nil
	case n == 0:
		tmp := make([]byte, 0)
		return &tmp, nil
	case n > rd.avail():
		return nil, decodingError{}
	default:
		tmp := rd.raw[rd.off : rd.off+n]
		return &tmp, nil
	}
}

func (rd *realDecoder) getArrayCount() (int, error) {
	if rd.avail() < 4 {
		return -1, decodingError{}
	}
	tmp := int(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4
	if tmp > rd.avail() || tmp > 2*math.MaxUint16 {
		return -1, decodingError{}
	}
	return tmp, nil
}

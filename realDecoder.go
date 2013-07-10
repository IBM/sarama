package kafka

import (
	"encoding/binary"
	"errors"
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
		return -1, errors.New("kafka getInt16: not enough data")
	}
	tmp := int16(binary.BigEndian.Uint16(rd.raw[rd.off:]))
	rd.off += 2
	return tmp, nil
}

func (rd *realDecoder) getInt32() (int32, error) {
	if rd.avail() < 4 {
		return -1, errors.New("kafka getInt32: not enough data")
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
		return nil, errors.New("kafka getString: invalid negative length")
	case n == -1:
		return nil, nil
	case n == 0:
		return new(string), nil
	case n > rd.avail():
		return nil, errors.New("kafka getString: not enough data")
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
		return nil, errors.New("kafka getBytes: invalid negative length")
	case n == -1:
		return nil, nil
	case n == 0:
		tmp := make([]byte, 0)
		return &tmp, nil
	case n > rd.avail():
		return nil, errors.New("kafka getString: not enough data")
	default:
		tmp := rd.raw[rd.off : rd.off+n]
		return &tmp, nil
	}
}

func (rd *realDecoder) getArrayCount() (int, error) {
	if rd.avail() < 4 {
		return -1, errors.New("kafka getArrayCount: not enough data")
	}
	tmp := int(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4
	if tmp > rd.avail() || tmp > 2*math.MaxUint16 {
		return -1, errors.New("kafka getArrayCount: unreasonably long array")
	}
	return tmp, nil
}

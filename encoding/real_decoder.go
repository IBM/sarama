package encoding

import (
	"encoding/binary"
	"math"
)

type realDecoder struct {
	raw   []byte
	off   int
	stack []PushDecoder
}

// primitives

func (rd *realDecoder) GetInt8() (int8, error) {
	if rd.Remaining() < 1 {
		rd.off = len(rd.raw)
		return -1, InsufficientData
	}
	tmp := int8(rd.raw[rd.off])
	rd.off += 1
	return tmp, nil
}

func (rd *realDecoder) GetInt16() (int16, error) {
	if rd.Remaining() < 2 {
		rd.off = len(rd.raw)
		return -1, InsufficientData
	}
	tmp := int16(binary.BigEndian.Uint16(rd.raw[rd.off:]))
	rd.off += 2
	return tmp, nil
}

func (rd *realDecoder) GetInt32() (int32, error) {
	if rd.Remaining() < 4 {
		rd.off = len(rd.raw)
		return -1, InsufficientData
	}
	tmp := int32(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4
	return tmp, nil
}

func (rd *realDecoder) GetInt64() (int64, error) {
	if rd.Remaining() < 8 {
		rd.off = len(rd.raw)
		return -1, InsufficientData
	}
	tmp := int64(binary.BigEndian.Uint64(rd.raw[rd.off:]))
	rd.off += 8
	return tmp, nil
}

func (rd *realDecoder) GetArrayLength() (int, error) {
	if rd.Remaining() < 4 {
		rd.off = len(rd.raw)
		return -1, InsufficientData
	}
	tmp := int(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4
	if tmp > rd.Remaining() {
		rd.off = len(rd.raw)
		return -1, InsufficientData
	} else if tmp > 2*math.MaxUint16 {
		return -1, DecodingError
	}
	return tmp, nil
}

// collections

func (rd *realDecoder) GetBytes() ([]byte, error) {
	tmp, err := rd.GetInt32()

	if err != nil {
		return nil, err
	}

	n := int(tmp)

	switch {
	case n < -1:
		return nil, DecodingError
	case n == -1:
		return nil, nil
	case n == 0:
		return make([]byte, 0), nil
	case n > rd.Remaining():
		rd.off = len(rd.raw)
		return nil, InsufficientData
	default:
		tmp := rd.raw[rd.off : rd.off+n]
		rd.off += n
		return tmp, nil
	}
}

func (rd *realDecoder) GetString() (string, error) {
	tmp, err := rd.GetInt16()

	if err != nil {
		return "", err
	}

	n := int(tmp)

	switch {
	case n < -1:
		return "", DecodingError
	case n == -1:
		return "", nil
	case n == 0:
		return "", nil
	case n > rd.Remaining():
		rd.off = len(rd.raw)
		return "", InsufficientData
	default:
		tmp := string(rd.raw[rd.off : rd.off+n])
		rd.off += n
		return tmp, nil
	}
}

func (rd *realDecoder) GetInt32Array() ([]int32, error) {
	if rd.Remaining() < 4 {
		rd.off = len(rd.raw)
		return nil, InsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4

	var ret []int32 = nil
	if rd.Remaining() < 4*n {
		rd.off = len(rd.raw)
		return nil, InsufficientData
	} else if n > 0 {
		ret = make([]int32, n)
		for i := range ret {
			ret[i] = int32(binary.BigEndian.Uint32(rd.raw[rd.off:]))
			rd.off += 4
		}
	}
	return ret, nil
}

func (rd *realDecoder) GetInt64Array() ([]int64, error) {
	if rd.Remaining() < 4 {
		rd.off = len(rd.raw)
		return nil, InsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4

	var ret []int64 = nil
	if rd.Remaining() < 8*n {
		rd.off = len(rd.raw)
		return nil, InsufficientData
	} else if n > 0 {
		ret = make([]int64, n)
		for i := range ret {
			ret[i] = int64(binary.BigEndian.Uint64(rd.raw[rd.off:]))
			rd.off += 8
		}
	}
	return ret, nil
}

// subsets

func (rd *realDecoder) Remaining() int {
	return len(rd.raw) - rd.off
}

func (rd *realDecoder) GetSubset(length int) (PacketDecoder, error) {
	if length > rd.Remaining() {
		rd.off = len(rd.raw)
		return nil, InsufficientData
	}

	start := rd.off
	rd.off += length
	return &realDecoder{raw: rd.raw[start:rd.off]}, nil
}

// stacks

func (rd *realDecoder) Push(in PushDecoder) error {
	in.SaveOffset(rd.off)

	reserve := in.ReserveLength()
	if rd.Remaining() < reserve {
		rd.off = len(rd.raw)
		return InsufficientData
	}

	rd.stack = append(rd.stack, in)

	rd.off += reserve

	return nil
}

func (rd *realDecoder) Pop() error {
	// this is go's ugly pop pattern (the inverse of append)
	in := rd.stack[len(rd.stack)-1]
	rd.stack = rd.stack[:len(rd.stack)-1]

	return in.Check(rd.off, rd.raw)
}

package kafka

import (
	"encoding/binary"
	"errors"
	"math"
)

func stringLen(in *string) (n int, err error) {
	if in == nil {
		return 2, nil
	}
	n = len(*in)
	if n > math.MaxInt16 {
		return -1, errors.New("kafka: string too long to encode")
	}
	return 2 + n, nil
}

func encodeString(in *string, buf []byte) (err error) {
	if len(buf) < 2 {
		return errors.New("kafka: buffer too short to encode any string")
	}
	n := -1
	if in != nil {
		n = len(*in)
	}
	if n > math.MaxInt16 {
		return errors.New("kafka: string too long to encode")
	}
	if n > len(buf) {
		return errors.New("kafka: buffer too short to encode string")
	}
	binary.BigEndian.PutUint16(buf, uint16(n))
	if n > 0 {
		copy(buf[2:], *in)
	}
	return nil
}

func decodeString(buf []byte) (out *string, err error) {
	if len(buf) < 2 {
		return nil, errors.New("kafka: buffer too short to contain string")
	}
	n := int16(binary.BigEndian.Uint16(buf))
	switch {
	case n < -1:
		return nil, errors.New("kafka: invalid negative string length")
	case n == -1:
		return nil, nil
	case n == 0:
		emptyString := ""
		return &emptyString, nil
	case int(n) > len(buf)-2:
		return nil, errors.New("kafka: buffer too short to decode string")
	default:
		result := string(buf[2:])
		return &result, nil
	}
}

func bytesLen(in *[]byte) (n int, err error) {
	if in == nil {
		return 4, nil
	}
	n = len(*in)
	if n > math.MaxInt32 {
		return -1, errors.New("kafka: bytes too long to encode")
	}
	return 4 + n, nil
}

func encodeBytes(in *[]byte, buf []byte) (err error) {
	if len(buf) < 4 {
		return errors.New("kafka: buffer too short to encode any bytes")
	}
	n := -1
	if in != nil {
		n = len(*in)
	}
	if n > math.MaxInt32 {
		return errors.New("kafka: bytes too long to encode")
	}
	if n > len(buf) {
		return errors.New("kafka: buffer too short to encode bytes")
	}
	binary.BigEndian.PutUint32(buf, uint32(n))
	if n > 0 {
		copy(buf[4:], *in)
	}
	return nil
}

func decodebyte(buf []byte) (out *[]byte, err error) {
	if len(buf) < 4 {
		return nil, errors.New("kafka: buffer too short to contain bytes")
	}
	n := int32(binary.BigEndian.Uint32(buf))
	switch {
	case n < -1:
		return nil, errors.New("kafka: invalid negative byte length")
	case n == -1:
		return nil, nil
	case n == 0:
		emptyBytes := make([]byte, 0)
		return &emptyBytes, nil
	case int(n) > len(buf)-4:
		return nil, errors.New("kafka: buffer too short to decode bytes")
	default:
		result := buf[4:]
		return &result, nil
	}
}

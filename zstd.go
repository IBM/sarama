package sarama

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

type ZstdEncoderParams struct {
	Level int
}
type ZstdDecoderParams struct {
}

var zstdDecMap sync.Map

var zstdAvailableEncoders sync.Map

func getZstdEncoderPool(params ZstdEncoderParams) *sync.Pool {
	if c, ok := zstdAvailableEncoders.Load(params); ok {
		return c.(*sync.Pool)
	}

	encoderLevel := zstd.SpeedDefault
	if params.Level != CompressionLevelDefault {
		encoderLevel = zstd.EncoderLevelFromZstd(params.Level)
	}

	pool := &sync.Pool{
		New: func() any {
			enc, _ := zstd.NewWriter(nil, zstd.WithZeroFrames(true),
				zstd.WithEncoderLevel(encoderLevel),
				zstd.WithEncoderConcurrency(1))
			return enc
		},
	}

	c, _ := zstdAvailableEncoders.LoadOrStore(params, pool)
	return c.(*sync.Pool)
}

func getZstdEncoder(params ZstdEncoderParams) *zstd.Encoder {
	return getZstdEncoderPool(params).Get().(*zstd.Encoder)
}

func releaseEncoder(params ZstdEncoderParams, enc *zstd.Encoder) {
	getZstdEncoderPool(params).Put(enc)
}

func getDecoder(params ZstdDecoderParams) *zstd.Decoder {
	if ret, ok := zstdDecMap.Load(params); ok {
		return ret.(*zstd.Decoder)
	}
	// It's possible to race and create multiple new readers.
	// Only one will survive GC after use.
	zstdDec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	zstdDecMap.Store(params, zstdDec)
	return zstdDec
}

func zstdDecompress(params ZstdDecoderParams, dst, src []byte) ([]byte, error) {
	return getDecoder(params).DecodeAll(src, dst)
}

func zstdCompress(params ZstdEncoderParams, dst, src []byte) ([]byte, error) {
	enc := getZstdEncoder(params)
	out := enc.EncodeAll(src, dst)
	releaseEncoder(params, enc)
	return out, nil
}

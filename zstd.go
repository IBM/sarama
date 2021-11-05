package sarama

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
)

var zstdEncoderConcurrent int32

// SetZstdEncoderConcurrent allow to manage counter of encoders in zstd writer
func SetZstdEncoderConcurrent(concurrent int) {
	atomic.StoreInt32(&zstdEncoderConcurrent, int32(concurrent))
	recreateEncoders()
}

type ZstdEncoderParams struct {
	Level int
}
type ZstdDecoderParams struct {
}

var zstdEncMap, zstdDecMap sync.Map

func getEncoder(params ZstdEncoderParams) *zstd.Encoder {
	if ret, ok := zstdEncMap.Load(params); ok {
		return ret.(*zstd.Encoder)
	}
	// It's possible to race and create multiple new writers.
	// Only one will survive GC after use.
	zstdEnc := newEncoder(params)
	zstdEncMap.Store(params, zstdEnc)
	return zstdEnc
}

func recreateEncoders() {
	zstdEncMap.Range(func(key, _ interface{}) bool {
		params := key.(ZstdEncoderParams)
		zstdEnc := newEncoder(params)
		zstdEncMap.Store(params, zstdEnc)
		return true
	})
}

func newEncoder(params ZstdEncoderParams) *zstd.Encoder {
	encoderLevel := zstd.SpeedDefault
	if params.Level != CompressionLevelDefault {
		encoderLevel = zstd.EncoderLevelFromZstd(params.Level)
	}
	concurrent := int(atomic.LoadInt32(&zstdEncoderConcurrent))
	if concurrent <= 0 {
		concurrent = runtime.GOMAXPROCS(0)
	}
	zstdEnc, _ := zstd.NewWriter(nil, zstd.WithZeroFrames(true),
		zstd.WithEncoderConcurrency(concurrent),
		zstd.WithEncoderLevel(encoderLevel))
	return zstdEnc
}

func getDecoder(params ZstdDecoderParams) *zstd.Decoder {
	if ret, ok := zstdDecMap.Load(params); ok {
		return ret.(*zstd.Decoder)
	}
	// It's possible to race and create multiple new readers.
	// Only one will survive GC after use.
	zstdDec, _ := zstd.NewReader(nil)
	zstdDecMap.Store(params, zstdDec)
	return zstdDec
}

func zstdDecompress(params ZstdDecoderParams, dst, src []byte) ([]byte, error) {
	return getDecoder(params).DecodeAll(src, dst)
}

func zstdCompress(params ZstdEncoderParams, dst, src []byte) ([]byte, error) {
	return getEncoder(params).EncodeAll(src, dst), nil
}

package sarama

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

var zstdEncMap sync.Map

var (
	zstdDec, _ = zstd.NewReader(nil)
)

func getEncoder(level int) *zstd.Encoder {
	if ret, ok := zstdEncMap.Load(level); ok {
		return ret.(*zstd.Encoder)
	}
	// It's possible to race and create multiple new writers.
	// Only one will survive GC after use.
	encoderLevel := zstd.SpeedDefault
	if level != CompressionLevelDefault {
		encoderLevel = zstd.EncoderLevelFromZstd(level)
	}
	zstdEnc, _ := zstd.NewWriter(nil, zstd.WithZeroFrames(true),
		zstd.WithEncoderLevel(encoderLevel))
	zstdEncMap.Store(level, zstdEnc)
	return zstdEnc
}

func zstdDecompress(dst, src []byte) ([]byte, error) {
	return zstdDec.DecodeAll(src, dst)
}

func zstdCompress(level int, dst, src []byte) ([]byte, error) {
	return getEncoder(level).EncodeAll(src, dst), nil
}

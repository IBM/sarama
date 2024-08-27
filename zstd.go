package sarama

import (
	"runtime"
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

var zstdCheckedOutEncoders int
var zstdMutex = &sync.Mutex{}
var zstdEncoderReturned = sync.NewCond(zstdMutex)
var zstdTestingDisableConcurrencyLimit bool

func getZstdEncoderChannel(params ZstdEncoderParams) chan *zstd.Encoder {
	if c, ok := zstdAvailableEncoders.Load(params); ok {
		return c.(chan *zstd.Encoder)
	}
	limit := runtime.GOMAXPROCS(0)
	c, _ := zstdAvailableEncoders.LoadOrStore(params, make(chan *zstd.Encoder, limit))
	return c.(chan *zstd.Encoder)
}

func newZstdEncoder(params ZstdEncoderParams) *zstd.Encoder {
	encoderLevel := zstd.SpeedDefault
	if params.Level != CompressionLevelDefault {
		encoderLevel = zstd.EncoderLevelFromZstd(params.Level)
	}
	zstdEnc, _ := zstd.NewWriter(nil, zstd.WithZeroFrames(true),
		zstd.WithEncoderLevel(encoderLevel),
		zstd.WithEncoderConcurrency(1))
	return zstdEnc
}

func getZstdEncoder(params ZstdEncoderParams) *zstd.Encoder {

	zstdMutex.Lock()
	defer zstdMutex.Unlock()

	limit := runtime.GOMAXPROCS(0)
	for zstdCheckedOutEncoders >= limit && !zstdTestingDisableConcurrencyLimit {
		zstdEncoderReturned.Wait()
		limit = runtime.GOMAXPROCS(0)
	}

	zstdCheckedOutEncoders += 1

	select {
	case enc := <-getZstdEncoderChannel(params):
		return enc
	default:
		return newZstdEncoder(params)
	}
}

func releaseEncoder(params ZstdEncoderParams, enc *zstd.Encoder) {
	zstdMutex.Lock()

	zstdCheckedOutEncoders -= 1

	select {
	case getZstdEncoderChannel(params) <- enc:
	default:
	}

	zstdEncoderReturned.Signal()

	zstdMutex.Unlock()
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

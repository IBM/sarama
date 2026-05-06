package sarama

import (
	"runtime"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// ZstdMaxBufferedEncoders bounds the number of idle zstd encoders Sarama
// retains per ZstdEncoderParams. The previous bound of 1 forced any
// concurrent caller after the first to allocate (and discard) a fresh
// encoder per batch; defaulting to runtime.GOMAXPROCS lets steady-state
// per-CPU concurrency reuse encoders while still bounding per-level memory
// on high-core hosts. Each retained encoder is constructed with
// WithEncoderConcurrency(1) to keep its own memory footprint small.
//
// The value is read on first use of each ZstdEncoderParams; changes after
// that point only affect compression levels not yet seen. To override the
// default, set this variable before any zstd compression takes place.
var ZstdMaxBufferedEncoders = max(runtime.GOMAXPROCS(0), 1)

type ZstdEncoderParams struct {
	Level int
}
type ZstdDecoderParams struct {
}

var zstdDecMap sync.Map

var (
	zstdAvailableEncoders sync.Map
	zstdEncoderInitMu     sync.Mutex
)

// getZstdEncoderChannel returns the buffered channel that retains idle zstd
// encoders for the given params. The slow path holds a single global mutex
// and re-checks the sync.Map under the lock so that a stampede of goroutines
// arriving for the same not-yet-seen ZstdEncoderParams cannot create
// multiple competing channels.
func getZstdEncoderChannel(params ZstdEncoderParams) chan *zstd.Encoder {
	if c, ok := zstdAvailableEncoders.Load(params); ok {
		return c.(chan *zstd.Encoder)
	}

	zstdEncoderInitMu.Lock()
	defer zstdEncoderInitMu.Unlock()

	if c, ok := zstdAvailableEncoders.Load(params); ok {
		return c.(chan *zstd.Encoder)
	}

	size := ZstdMaxBufferedEncoders
	if size < 1 {
		size = 1
	}
	ch := make(chan *zstd.Encoder, size)
	zstdAvailableEncoders.Store(params, ch)
	return ch
}

func newZstdEncoder(params ZstdEncoderParams) *zstd.Encoder {
	encoderLevel := zstd.SpeedDefault
	if params.Level != CompressionLevelDefault {
		encoderLevel = zstd.EncoderLevelFromZstd(params.Level)
	}
	enc, _ := zstd.NewWriter(nil,
		zstd.WithZeroFrames(true),
		zstd.WithEncoderLevel(encoderLevel),
		zstd.WithEncoderConcurrency(1))
	return enc
}

func getZstdEncoder(params ZstdEncoderParams) *zstd.Encoder {
	select {
	case enc := <-getZstdEncoderChannel(params):
		return enc
	default:
		return newZstdEncoder(params)
	}
}

func releaseEncoder(params ZstdEncoderParams, enc *zstd.Encoder) {
	select {
	case getZstdEncoderChannel(params) <- enc:
	default:
		// pool is at capacity; let the encoder be garbage collected.
	}
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

package sarama

import (
	"runtime"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func BenchmarkZstdMemoryConsumption(b *testing.B) {
	params := ZstdEncoderParams{Level: 9}
	buf := make([]byte, 1024*1024)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte((i / 256) + (i * 257))
	}

	cpus := 96

	gomaxprocsBackup := runtime.GOMAXPROCS(cpus)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 2*cpus; j++ {
			_, _ = zstdCompress(params, 1, nil, buf)
		}
		// drain the buffered encoder
		getZstdEncoder(params, 1)
		// previously this would be achieved with
		// zstdEncMap.Delete(params)
	}
	runtime.GOMAXPROCS(gomaxprocsBackup)
}

func TestMaxBufferedEncodersCapacity(t *testing.T) {
	maxBufferedEncoders := 10
	params := ZstdEncoderParams{Level: 3}
	buf := make([]byte, 1024*1024)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte((i / 256) + (i * 257))
	}

	var encoders []*zstd.Encoder = make([]*zstd.Encoder, maxBufferedEncoders)
	var ch chan *zstd.Encoder
	for i := 0; i < maxBufferedEncoders; i++ {
		encoders[i] = getZstdEncoder(params, maxBufferedEncoders)
	}
	ch = getZstdEncoderChannel(params, maxBufferedEncoders)
	// channel should be empty at the moment
	if len(ch) != 0 {
		t.Error("Expects channel len to be ", 0, ", got ", len(ch))
	}
	if cap(ch) != maxBufferedEncoders {
		t.Error("Expects channel cap to be ", maxBufferedEncoders, ", got ", cap(ch))
	}
	// this adds the encoders to the channel
	for i := 0; i < maxBufferedEncoders; i++ {
		releaseEncoder(params, maxBufferedEncoders, encoders[i])
	}
	if len(ch) != maxBufferedEncoders {
		t.Error("Expects channel len to be ", maxBufferedEncoders, ", got ", len(ch))
	}
	// Drain the channel
	for i := 0; i < maxBufferedEncoders; i++ {
		encoders[i] = getZstdEncoder(params, maxBufferedEncoders)
	}
	ch = getZstdEncoderChannel(params, maxBufferedEncoders)
	// channel should be empty at the moment
	if len(ch) != 0 {
		t.Error("Expects channel len to be ", 0, ", got ", len(ch))
	}
}

func TestMaxBufferedEncodersDefault(t *testing.T) {
	var maxBufferedEncoders int
	encoders := zstdMaxBufferedEncoders(maxBufferedEncoders)
	if encoders != 1 {
		t.Error("Expects encoders to be ", 1, ", got ", encoders)
	}
}

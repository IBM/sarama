package sarama

import (
	"runtime"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func BenchmarkZstdMemoryConsumption(b *testing.B) {
	params := ZstdEncoderParams{Level: 9, MaxBufferedEncoders: 1}
	buf := make([]byte, 1024*1024)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte((i / 256) + (i * 257))
	}

	cpus := 96

	gomaxprocsBackup := runtime.GOMAXPROCS(cpus)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 2*cpus; j++ {
			_, _ = zstdCompress(params, nil, buf)
		}
		// drain the buffered encoder
		getZstdEncoder(params)
		// previously this would be achieved with
		// zstdEncMap.Delete(params)
	}
	runtime.GOMAXPROCS(gomaxprocsBackup)
}

func TestMaxBufferedEncodersCapacity(t *testing.T) {
	num := 10
	params := ZstdEncoderParams{Level: 3, MaxBufferedEncoders: num}
	buf := make([]byte, 1024*1024)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte((i / 256) + (i * 257))
	}

	var encoders []*zstd.Encoder = make([]*zstd.Encoder, num)
	var ch chan *zstd.Encoder
	for i := 0; i < num; i++ {
		encoders[i] = getZstdEncoder(params)
	}
	ch = getZstdEncoderChannel(params)
	// channel should be empty at the moment
	if len(ch) != 0 {
		t.Error("Expects channel len to be ", 0, ", got ", len(ch))
	}
	if cap(ch) != num {
		t.Error("Expects channel cap to be ", num, ", got ", cap(ch))
	}
	// this adds the encoders to the channel
	for i := 0; i < num; i++ {
		releaseEncoder(params, encoders[i])
	}
	if len(ch) != num {
		t.Error("Expects channel len to be ", num, ", got ", len(ch))
	}
	// Drain the channel
	for i := 0; i < num; i++ {
		encoders[i] = getZstdEncoder(params)
	}
	ch = getZstdEncoderChannel(params)
	// channel should be empty at the moment
	if len(ch) != 0 {
		t.Error("Expects channel len to be ", 0, ", got ", len(ch))
	}
}

func TestMaxBufferedEncodersDefault(t *testing.T) {
	params := ZstdEncoderParams{}
	encoders := getZstdMaxBufferedEncoders(params)
	if encoders != 1 {
		t.Error("Expects encoders to be ", 1, ", got ", encoders)
	}
}

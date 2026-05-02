//go:build !functional

package sarama

import (
	"runtime"
	"testing"
)

func BenchmarkZstdMemoryConsumption(b *testing.B) {
	params := ZstdEncoderParams{Level: 9}
	buf := make([]byte, 1024*1024)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte((i / 256) + (i * 257))
	}

	cpus := 96

	b.Run("no_drain", func(b *testing.B) {
		b.ReportAllocs()

		gomaxprocsBackup := runtime.GOMAXPROCS(cpus)
		for b.Loop() {
			_, _ = zstdCompress(params, nil, buf)
		}
		runtime.GOMAXPROCS(gomaxprocsBackup)
	})

	// Drops the buffered encoder so we can measure cold-start allocation
	b.Run("with_drain", func(b *testing.B) {
		b.ReportAllocs()

		gomaxprocsBackup := runtime.GOMAXPROCS(cpus)
		for b.Loop() {
			_, _ = zstdCompress(params, nil, buf)
			_ = getZstdEncoder(params)
		}
		runtime.GOMAXPROCS(gomaxprocsBackup)

	})

	// Concurrent encodes, which historically would allocate excessively
	b.Run("concurrent", func(b *testing.B) {
		b.ReportAllocs()

		gomaxprocsBackup := runtime.GOMAXPROCS(cpus)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = zstdCompress(params, nil, buf)
			}
		})
		runtime.GOMAXPROCS(gomaxprocsBackup)
	})
}

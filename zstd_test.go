//go:build !functional

package sarama

import (
	"testing"
)

func BenchmarkZstdMemoryConsumption(b *testing.B) {
	params := ZstdEncoderParams{Level: 9}
	buf := make([]byte, 1024*1024)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte((i / 256) + (i * 257))
	}

	b.Run("no_drain", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = zstdCompress(params, nil, buf)
		}
	})

	// Drops the buffered encoder so we can measure cold-start allocation.
	b.Run("with_drain", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = zstdCompress(params, nil, buf)
			_ = getZstdEncoder(params)
		}
	})

	// Concurrent encodes, which historically allocated a fresh encoder per
	// batch for every concurrent caller beyond the first. RunParallel uses
	// GOMAXPROCS goroutines by default; SetParallelism scales that ratio for
	// hosts where exercising more concurrency than CPUs is interesting.
	b.Run("concurrent", func(b *testing.B) {
		b.ReportAllocs()
		b.SetParallelism(2)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = zstdCompress(params, nil, buf)
			}
		})
	})
}

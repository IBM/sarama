package sarama

import (
	"runtime"
	"sync"
	"testing"
)

// BenchmarkZstdMemoryConsumption benchmarks the memory consumption of the zstd encoder under the following constraints
// 1. The encoder is created with a high compression level
// 2. The encoder is used to compress a 1MB buffer
// 3. We emulate a 96 core system
// In other words: we test the compression memory and cpu efficiency under minimal parallelism
func BenchmarkZstdMemoryConsumption(b *testing.B) {
	params := ZstdEncoderParams{Level: 9}
	buf := make([]byte, 1024*1024)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte((i / 256) + (i * 257))
	}

	cpus := 96

	gomaxprocsBackup := runtime.GOMAXPROCS(cpus)
	defer runtime.GOMAXPROCS(gomaxprocsBackup)

	b.SetBytes(int64(len(buf) * 2 * cpus))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 2*cpus; j++ {
			_, _ = zstdCompress(params, nil, buf)
		}
		// drain the buffered encoder so that we get a fresh one for the next run
		zstdAvailableEncoders.Delete(params)
	}

	b.ReportMetric(float64(cpus), "(gomaxprocs)")
	b.ReportMetric(float64(1), "(goroutines)")
}

// BenchmarkZstdMemoryConsumptionConcurrency benchmarks the memory consumption of the zstd encoder under the following constraints
// 1. The encoder is created with a high compression level
// 2. The encoder is used to compress a 1MB buffer
// 3. We emulate a 2 core system
// 4. We create 1000 goroutines that compress the buffer 2 times each
// In summary: we test the compression memory and cpu efficiency under extreme concurrency
func BenchmarkZstdMemoryConsumptionConcurrency(b *testing.B) {
	params := ZstdEncoderParams{Level: 9}
	buf := make([]byte, 1024*1024)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte((i / 256) + (i * 257))
	}

	cpus := 4
	goroutines := 256

	gomaxprocsBackup := runtime.GOMAXPROCS(cpus)
	defer runtime.GOMAXPROCS(gomaxprocsBackup)

	b.ReportMetric(float64(cpus), "(gomaxprocs)")
	b.ResetTimer()
	b.SetBytes(int64(len(buf) * goroutines))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// create n goroutines, wait until all start and then signal them to start compressing
		var start sync.WaitGroup
		var done sync.WaitGroup
		start.Add(goroutines)
		done.Add(goroutines)
		for j := 0; j < goroutines; j++ {
			go func() {
				start.Done()
				start.Wait()
				_, _ = zstdCompress(params, nil, buf)
				done.Done()
			}()
			zstdAvailableEncoders.Delete(params)
		}
		done.Wait()
	}

	b.ReportMetric(float64(cpus), "(gomaxprocs)")
	b.ReportMetric(float64(goroutines), "(goroutines)")
}

// BenchmarkZstdMemoryNoConcurrencyLimit benchmarks the encoder behavior when the concurrency limit is disabled.
func BenchmarkZstdMemoryNoConcurrencyLimit(b *testing.B) {
	zstdTestingDisableConcurrencyLimit = true
	defer func() {
		zstdTestingDisableConcurrencyLimit = false
	}()

	params := ZstdEncoderParams{Level: 9}
	buf := make([]byte, 1024*1024)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte((i / 256) + (i * 257))
	}

	cpus := 4
	goroutines := 256

	gomaxprocsBackup := runtime.GOMAXPROCS(cpus)
	defer runtime.GOMAXPROCS(gomaxprocsBackup)

	b.ReportMetric(float64(cpus), "(gomaxprocs)")
	b.ResetTimer()
	b.SetBytes(int64(len(buf) * goroutines))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// create n goroutines, wait until all start and then signal them to start compressing
		var start sync.WaitGroup
		var done sync.WaitGroup
		start.Add(goroutines)
		done.Add(goroutines)
		for j := 0; j < goroutines; j++ {
			go func() {
				start.Done()
				start.Wait()
				_, _ = zstdCompress(params, nil, buf)
				done.Done()
			}()
			zstdAvailableEncoders.Delete(params)
		}
		done.Wait()
	}

	b.ReportMetric(float64(cpus), "(gomaxprocs)")
	b.ReportMetric(float64(goroutines), "(goroutines)")
}

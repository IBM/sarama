//go:build functional

package sarama

import (
	"fmt"
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"
)

const (
	producerBenchmarkWarmupMessages = 2000
	producerBenchmarkPayloadBytes   = 512
	producerBenchmarkTopic          = "test.64"
)

type producerBenchmarkProfile struct {
	name            string
	idempotent      bool
	maxOpenRequests int
}

func BenchmarkProducerProfiles(b *testing.B) {
	profiles := []producerBenchmarkProfile{
		{name: "idempotent_maxreq_1", idempotent: true, maxOpenRequests: 1},
		{name: "idempotent_maxreq_5", idempotent: true, maxOpenRequests: 5},
		{name: "best_effort_maxreq_1", idempotent: false, maxOpenRequests: 1},
		{name: "best_effort_maxreq_5", idempotent: false, maxOpenRequests: 5},
	}

	for _, profile := range profiles {
		b.Run(profile.name, func(b *testing.B) {
			runProducerProfileBenchmark(b, profile)
		})
	}
}

func runProducerProfileBenchmark(b *testing.B, profile producerBenchmarkProfile) {
	setupFunctionalTest(b)
	defer teardownFunctionalTest(b)

	config := NewFunctionalTestConfig()
	config.ClientID = fmt.Sprintf("producer-bench-%s", profile.name)
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = NewRoundRobinPartitioner
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 50 * time.Millisecond
	config.Producer.Idempotent = profile.idempotent
	config.Net.MaxOpenRequests = profile.maxOpenRequests

	producer, err := NewAsyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		b.Fatal(err)
	}
	defer safeClose(b, producer)

	payload := make([]byte, producerBenchmarkPayloadBytes)
	b.SetBytes(int64(len(payload)))

	if err := produceBatch(producer, producerBenchmarkWarmupMessages, payload, nil); err != nil {
		b.Fatalf("warmup failed: %v", err)
	}

	latency := metrics.NewHistogram(metrics.NewUniformSample(b.N))
	b.ResetTimer()
	if err := produceBatch(producer, b.N, payload, latency); err != nil {
		b.Fatalf("benchmark failed: %v", err)
	}
	b.StopTimer()

	if latency.Count() == 0 {
		return
	}

	recordProducerLatencyMetrics(b, latency)
}

func produceBatch(producer AsyncProducer, count int, payload []byte, latency metrics.Histogram) error {
	errCh := make(chan error, 1)
	acksDone := make(chan struct{})

	go func() {
		defer close(acksDone)
		delivered := 0
		for delivered < count {
			select {
			case err, ok := <-producer.Errors():
				if !ok {
					errCh <- fmt.Errorf("producer error channel closed while awaiting acks")
					return
				}
				if err == nil {
					errCh <- fmt.Errorf("received nil producer error while awaiting acks")
					return
				}
				errCh <- err.Err
				return
			case ack, ok := <-producer.Successes():
				if !ok {
					errCh <- fmt.Errorf("producer successes channel closed before all messages were acknowledged")
					return
				}
				if latency != nil {
					if start, ok := ack.Metadata.(time.Time); ok {
						latency.Update(time.Since(start).Nanoseconds())
					}
				}
				delivered++
			}
		}
	}()

	for range count {
		msg := &ProducerMessage{
			Topic: producerBenchmarkTopic,
			Value: ByteEncoder(payload),
		}
		if latency != nil {
			msg.Metadata = time.Now()
		}

		select {
		case producer.Input() <- msg:
		case err := <-errCh:
			return err
		}
	}

	select {
	case <-acksDone:
		return nil
	case err := <-errCh:
		return err
	}
}

func recordProducerLatencyMetrics(b *testing.B, latency metrics.Histogram) {
	elapsed := b.Elapsed()
	p50 := time.Duration(latency.Percentile(0.50))
	p95 := time.Duration(latency.Percentile(0.95))
	p99 := time.Duration(latency.Percentile(0.99))

	b.ReportMetric(float64(p50)/float64(time.Millisecond), "p50_ms")
	b.ReportMetric(float64(p95)/float64(time.Millisecond), "p95_ms")
	b.ReportMetric(float64(p99)/float64(time.Millisecond), "p99_ms")

	if elapsed > 0 {
		tput := float64(latency.Count()) / elapsed.Seconds()
		b.ReportMetric(tput, "msgs_per_sec")
	}

	b.Logf("latency p50=%.2fms p95=%.2fms p99=%.2fms throughput=%.0f msg/s",
		float64(p50)/float64(time.Millisecond),
		float64(p95)/float64(time.Millisecond),
		float64(p99)/float64(time.Millisecond),
		float64(latency.Count())/elapsed.Seconds(),
	)
}

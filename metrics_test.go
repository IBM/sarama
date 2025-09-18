//go:build !functional

package sarama

import (
	"testing"

	"github.com/rcrowley/go-metrics"
)

func TestGetOrRegisterHistogram(t *testing.T) {
	metricRegistry := metrics.NewRegistry()
	histogram := getOrRegisterHistogram("name", metricRegistry)

	if histogram == nil {
		t.Error("Unexpected nil histogram")
	}

	// Fetch the metric
	foundHistogram := metricRegistry.Get("name")

	if foundHistogram != histogram {
		t.Error("Unexpected different histogram", foundHistogram, histogram)
	}

	// Try to register the metric again
	sameHistogram := getOrRegisterHistogram("name", metricRegistry)

	if sameHistogram != histogram {
		t.Error("Unexpected different histogram", sameHistogram, histogram)
	}
}

func TestGetMetricNameForBroker(t *testing.T) {
	metricName := getMetricNameForBroker("name", &Broker{id: 1})

	if metricName != "name-for-broker-1" {
		t.Error("Unexpected metric name", metricName)
	}
}

func Benchmark_getMetricNameForTopic(b *testing.B) {
	b.ReportAllocs()

	for b.Loop() {
		name := getMetricNameForTopic("sarama", "says.hello")
		if name != "sarama-for-topic-says_hello" {
			b.Fail()
		}
	}
}

func Benchmark_getMetricNameForBroker(b *testing.B) {
	broker := &Broker{id: 1965}

	b.ReportAllocs()

	for b.Loop() {
		name := getMetricNameForBroker("summer", broker)
		if name != "summer-for-broker-1965" {
			b.Fail()
		}
	}
}

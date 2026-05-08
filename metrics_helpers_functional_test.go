//go:build functional

package sarama

import (
	"testing"

	"github.com/rcrowley/go-metrics"
)

func (m *metricValidators) registerForGlobalAndTopic(topic string, validator *metricValidator) {
	m.register(&metricValidator{validator.name, validator.validator})
	m.register(&metricValidator{getMetricNameForTopic(validator.name, topic), validator.validator})
}

func minCountMeterValidator(name string, minCount int) *metricValidator {
	return meterValidator(name, func(t *testing.T, meter metrics.Meter) {
		t.Helper()
		count := meter.Count()
		if count < int64(minCount) {
			t.Errorf("Expected meter metric '%s' count >= %d, got %d", name, minCount, count)
		}
	})
}

func minCountHistogramValidator(name string, minCount int) *metricValidator {
	return histogramValidator(name, func(t *testing.T, histogram metrics.Histogram) {
		t.Helper()
		count := histogram.Count()
		if count < int64(minCount) {
			t.Errorf("Expected histogram metric '%s' count >= %d, got %d", name, minCount, count)
		}
	})
}

func minValHistogramValidator(name string, minMin int) *metricValidator {
	return histogramValidator(name, func(t *testing.T, histogram metrics.Histogram) {
		t.Helper()
		min := int(histogram.Min())
		if min < minMin {
			t.Errorf("Expected histogram metric '%s' min >= %d, got %d", name, minMin, min)
		}
	})
}

func maxValHistogramValidator(name string, maxMax int) *metricValidator {
	return histogramValidator(name, func(t *testing.T, histogram metrics.Histogram) {
		t.Helper()
		max := int(histogram.Max())
		if max > maxMax {
			t.Errorf("Expected histogram metric '%s' max <= %d, got %d", name, maxMax, max)
		}
	})
}

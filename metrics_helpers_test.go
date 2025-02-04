package sarama

import (
	"testing"

	"github.com/rcrowley/go-metrics"
)

// Common type and functions for metric validation
type metricValidator struct {
	name      string
	validator func(*testing.T, interface{})
}

type metricValidators []*metricValidator

func newMetricValidators() metricValidators {
	return make([]*metricValidator, 0, 32)
}

func (m *metricValidators) register(validator *metricValidator) {
	*m = append(*m, validator)
}

func (m *metricValidators) registerForBroker(broker *Broker, validator *metricValidator) {
	m.register(&metricValidator{getMetricNameForBroker(validator.name, broker), validator.validator})
}

func (m *metricValidators) registerForGlobalAndTopic(topic string, validator *metricValidator) {
	m.register(&metricValidator{validator.name, validator.validator})
	m.register(&metricValidator{getMetricNameForTopic(validator.name, topic), validator.validator})
}

func (m *metricValidators) registerForAllBrokers(broker *Broker, validator *metricValidator) {
	m.register(validator)
	m.registerForBroker(broker, validator)
}

func (m metricValidators) run(t *testing.T, r metrics.Registry) {
	t.Helper()
	for _, metricValidator := range m {
		metric := r.Get(metricValidator.name)
		if metric == nil {
			t.Error("No metric named", metricValidator.name)
		} else {
			metricValidator.validator(t, metric)
		}
	}
}

func meterValidator(name string, extraValidator func(*testing.T, metrics.Meter)) *metricValidator {
	return &metricValidator{
		name: name,
		validator: func(t *testing.T, metric interface{}) {
			t.Helper()
			if meter, ok := metric.(metrics.Meter); !ok {
				t.Errorf("Expected meter metric for '%s', got %T", name, metric)
			} else {
				extraValidator(t, meter)
			}
		},
	}
}

func countMeterValidator(name string, expectedCount int) *metricValidator {
	return meterValidator(name, func(t *testing.T, meter metrics.Meter) {
		t.Helper()
		count := meter.Count()
		if count != int64(expectedCount) {
			t.Errorf("Expected meter metric '%s' count = %d, got %d", name, expectedCount, count)
		}
	})
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

func histogramValidator(name string, extraValidator func(*testing.T, metrics.Histogram)) *metricValidator {
	return &metricValidator{
		name: name,
		validator: func(t *testing.T, metric interface{}) {
			t.Helper()
			if histogram, ok := metric.(metrics.Histogram); !ok {
				t.Errorf("Expected histogram metric for '%s', got %T", name, metric)
			} else {
				extraValidator(t, histogram)
			}
		},
	}
}

func countHistogramValidator(name string, expectedCount int) *metricValidator {
	return histogramValidator(name, func(t *testing.T, histogram metrics.Histogram) {
		t.Helper()
		count := histogram.Count()
		if count != int64(expectedCount) {
			t.Errorf("Expected histogram metric '%s' count = %d, got %d", name, expectedCount, count)
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

//lint:ignore U1000 // this is used but only in unittests which are excluded by the integration build tag
func minMaxHistogramValidator(name string, expectedMin int, expectedMax int) *metricValidator {
	return histogramValidator(name, func(t *testing.T, histogram metrics.Histogram) {
		t.Helper()
		min := int(histogram.Min())
		if min != expectedMin {
			t.Errorf("Expected histogram metric '%s' min = %d, got %d", name, expectedMin, min)
		}
		max := int(histogram.Max())
		if max != expectedMax {
			t.Errorf("Expected histogram metric '%s' max = %d, got %d", name, expectedMax, max)
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

func counterValidator(name string, expectedCount int) *metricValidator {
	return &metricValidator{
		name: name,
		validator: func(t *testing.T, metric interface{}) {
			t.Helper()
			if counter, ok := metric.(metrics.Counter); !ok {
				t.Errorf("Expected counter metric for '%s', got %T", name, metric)
			} else {
				count := counter.Count()
				if count != int64(expectedCount) {
					t.Errorf("Expected counter metric '%s' count = %d, got %d", name, expectedCount, count)
				}
			}
		},
	}
}

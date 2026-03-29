package sarama

import (
	"sync"

	"github.com/rcrowley/go-metrics"
)

// MetricsProvider is an interface for pluggable metrics backends. Implementations
// can bridge sarama's metrics to OpenTelemetry, Prometheus, or any other system.
//
// When Config.MetricsProvider is non-nil it takes precedence over Config.MetricRegistry.
// The default value is nil, which preserves the existing rcrowley/go-metrics behavior.
type MetricsProvider interface {
	// GetCounter returns a named counter, creating it if it does not exist.
	GetCounter(name string) MetricsCounter

	// GetGauge returns a named gauge, creating it if it does not exist.
	GetGauge(name string) MetricsGauge

	// GetHistogram returns a named histogram, creating it if it does not exist.
	GetHistogram(name string) MetricsHistogram

	// GetMeter returns a named meter, creating it if it does not exist.
	GetMeter(name string) MetricsMeter

	// UnregisterAll removes all metrics that have been registered.
	UnregisterAll()
}

// MetricsCounter tracks a monotonically increasing value.
type MetricsCounter interface {
	Inc(int64)
	Count() int64
}

// MetricsGauge tracks a single int64 value that can go up and down.
type MetricsGauge interface {
	Update(int64)
	Value() int64
}

// MetricsHistogram tracks the distribution of a stream of int64 values.
type MetricsHistogram interface {
	Update(int64)
	Count() int64
}

// MetricsMeter tracks the rate of events over time.
type MetricsMeter interface {
	Mark(int64)
	Count() int64
}

// GoMetricsProvider wraps a rcrowley/go-metrics Registry to implement MetricsProvider.
// This is the default implementation used when Config.MetricsProvider is nil.
// It tracks which metrics it registers so UnregisterAll only removes its own,
// matching the cleanupRegistry pattern used elsewhere in sarama.
type GoMetricsProvider struct {
	registry metrics.Registry

	mu    sync.Mutex
	names map[string]struct{}
}

// NewGoMetricsProvider returns a MetricsProvider backed by the given go-metrics Registry.
func NewGoMetricsProvider(registry metrics.Registry) *GoMetricsProvider {
	return &GoMetricsProvider{
		registry: registry,
		names:    make(map[string]struct{}),
	}
}

func (p *GoMetricsProvider) track(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.names == nil {
		p.names = make(map[string]struct{})
	}

	p.names[name] = struct{}{}
}

func (p *GoMetricsProvider) GetCounter(name string) MetricsCounter {
	p.track(name)
	return metrics.GetOrRegisterCounter(name, p.registry)
}

func (p *GoMetricsProvider) GetGauge(name string) MetricsGauge {
	p.track(name)
	return metrics.GetOrRegisterGauge(name, p.registry)
}

func (p *GoMetricsProvider) GetHistogram(name string) MetricsHistogram {
	p.track(name)
	return p.registry.GetOrRegister(name, func() metrics.Histogram {
		return metrics.NewHistogram(metrics.NewExpDecaySample(metricsReservoirSize, metricsAlphaFactor))
	}).(metrics.Histogram)
}

func (p *GoMetricsProvider) GetMeter(name string) MetricsMeter {
	p.track(name)
	return metrics.GetOrRegisterMeter(name, p.registry)
}

func (p *GoMetricsProvider) UnregisterAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for name := range p.names {
		p.registry.Unregister(name)
		delete(p.names, name)
	}
}

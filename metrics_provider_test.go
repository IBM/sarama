//go:build !functional

package sarama

import (
	"sync"
	"testing"

	"github.com/rcrowley/go-metrics"
)

func TestGoMetricsProviderCounter(t *testing.T) {
	r := metrics.NewRegistry()
	p := NewGoMetricsProvider(r)

	c := p.NewCounter("test-counter")
	c.Inc(1)
	c.Inc(2)
	if c.Count() != 3 {
		t.Errorf("expected counter=3, got %d", c.Count())
	}

	// same name returns same instance
	c2 := p.NewCounter("test-counter")
	if c2.Count() != 3 {
		t.Errorf("expected same counter instance, got count=%d", c2.Count())
	}
}

func TestGoMetricsProviderGauge(t *testing.T) {
	r := metrics.NewRegistry()
	p := NewGoMetricsProvider(r)

	g := p.NewGauge("test-gauge")
	g.Update(42)
	if g.Value() != 42 {
		t.Errorf("expected gauge=42, got %d", g.Value())
	}
}

func TestGoMetricsProviderHistogram(t *testing.T) {
	r := metrics.NewRegistry()
	p := NewGoMetricsProvider(r)

	h := p.NewHistogram("test-histogram")
	h.Update(100)
	h.Update(200)
	if h.Count() != 2 {
		t.Errorf("expected histogram count=2, got %d", h.Count())
	}
}

func TestGoMetricsProviderMeter(t *testing.T) {
	r := metrics.NewRegistry()
	p := NewGoMetricsProvider(r)

	m := p.NewMeter("test-meter")
	m.Mark(5)
	m.Mark(3)
	if m.Count() != 8 {
		t.Errorf("expected meter count=8, got %d", m.Count())
	}
}

func TestGoMetricsProviderUnregisterAll(t *testing.T) {
	r := metrics.NewRegistry()
	p := NewGoMetricsProvider(r)

	p.NewCounter("c1")
	p.NewMeter("m1")

	// Register a metric directly on the registry (not via provider)
	metrics.GetOrRegisterCounter("external", r)

	p.UnregisterAll()

	if r.Get("c1") != nil {
		t.Error("expected c1 to be unregistered")
	}
	if r.Get("m1") != nil {
		t.Error("expected m1 to be unregistered")
	}
	// Metrics registered outside the provider should be preserved
	if r.Get("external") == nil {
		t.Error("expected external metric to survive UnregisterAll")
	}
}

// stubProvider is a minimal MetricsProvider for testing the interface contract.
type stubProvider struct {
	counters   map[string]*stubCounter
	gauges     map[string]*stubGauge
	histograms map[string]*stubHistogram
	meters     map[string]*stubMeter
	mu         sync.Mutex
}

type stubCounter struct{ val int64 }

func (c *stubCounter) Inc(v int64)  { c.val += v }
func (c *stubCounter) Count() int64 { return c.val }

type stubGauge struct{ val int64 }

func (g *stubGauge) Update(v int64) { g.val = v }
func (g *stubGauge) Value() int64   { return g.val }

type stubHistogram struct{ count int64 }

func (h *stubHistogram) Update(int64) { h.count++ }
func (h *stubHistogram) Count() int64 { return h.count }

type stubMeter struct{ count int64 }

func (m *stubMeter) Mark(v int64) { m.count += v }
func (m *stubMeter) Count() int64 { return m.count }

func newStubProvider() *stubProvider {
	return &stubProvider{
		counters:   make(map[string]*stubCounter),
		gauges:     make(map[string]*stubGauge),
		histograms: make(map[string]*stubHistogram),
		meters:     make(map[string]*stubMeter),
	}
}

func (p *stubProvider) NewCounter(name string) MetricsCounter {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.counters[name]; ok {
		return c
	}
	c := &stubCounter{}
	p.counters[name] = c
	return c
}

func (p *stubProvider) NewGauge(name string) MetricsGauge {
	p.mu.Lock()
	defer p.mu.Unlock()
	if g, ok := p.gauges[name]; ok {
		return g
	}
	g := &stubGauge{}
	p.gauges[name] = g
	return g
}

func (p *stubProvider) NewHistogram(name string) MetricsHistogram {
	p.mu.Lock()
	defer p.mu.Unlock()
	if h, ok := p.histograms[name]; ok {
		return h
	}
	h := &stubHistogram{}
	p.histograms[name] = h
	return h
}

func (p *stubProvider) NewMeter(name string) MetricsMeter {
	p.mu.Lock()
	defer p.mu.Unlock()
	if m, ok := p.meters[name]; ok {
		return m
	}
	m := &stubMeter{}
	p.meters[name] = m
	return m
}

func (p *stubProvider) UnregisterAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counters = make(map[string]*stubCounter)
	p.gauges = make(map[string]*stubGauge)
	p.histograms = make(map[string]*stubHistogram)
	p.meters = make(map[string]*stubMeter)
}

func TestCustomMetricsProvider(t *testing.T) {
	sp := newStubProvider()

	// Verify stub implements the interface
	var _ MetricsProvider = sp

	c := sp.NewCounter("requests")
	c.Inc(1)
	c.Inc(1)
	if c.Count() != 2 {
		t.Errorf("expected stub counter=2, got %d", c.Count())
	}

	m := sp.NewMeter("bytes")
	m.Mark(1024)
	if m.Count() != 1024 {
		t.Errorf("expected stub meter=1024, got %d", m.Count())
	}

	h := sp.NewHistogram("latency")
	h.Update(50)
	h.Update(100)
	if h.Count() != 2 {
		t.Errorf("expected stub histogram count=2, got %d", h.Count())
	}

	g := sp.NewGauge("connections")
	g.Update(10)
	if g.Value() != 10 {
		t.Errorf("expected stub gauge=10, got %d", g.Value())
	}
}

func TestConfigMetricsProviderDefault(t *testing.T) {
	config := NewConfig()
	if config.MetricsProvider != nil {
		t.Error("expected MetricsProvider to default to nil")
	}
	if config.MetricRegistry == nil {
		t.Error("expected MetricRegistry to be non-nil by default")
	}
}

func TestConfigMetricsProviderOverride(t *testing.T) {
	sp := newStubProvider()
	config := NewConfig()
	config.MetricsProvider = sp

	if err := config.Validate(); err != nil {
		t.Errorf("expected valid config, got: %v", err)
	}
}

func TestGoMetricsProviderConcurrentAccess(t *testing.T) {
	r := metrics.NewRegistry()
	p := NewGoMetricsProvider(r)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := p.NewCounter("concurrent-counter")
			c.Inc(1)
			m := p.NewMeter("concurrent-meter")
			m.Mark(1)
			h := p.NewHistogram("concurrent-histogram")
			h.Update(1)
		}()
	}
	wg.Wait()

	c := p.NewCounter("concurrent-counter")
	if c.Count() != 100 {
		t.Errorf("expected counter=100, got %d", c.Count())
	}
}

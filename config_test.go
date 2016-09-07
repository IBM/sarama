package sarama

import (
	"os"
	"testing"

	"github.com/rcrowley/go-metrics"
)

func TestDefaultConfigValidates(t *testing.T) {
	config := NewConfig()
	if err := config.Validate(); err != nil {
		t.Error(err)
	}
	if config.MetricRegistry == nil {
		t.Error("Expected non nil metrics.MetricRegistry, got nil")
	}
}

func TestInvalidClientIDConfigValidates(t *testing.T) {
	config := NewConfig()
	config.ClientID = "foo:bar"
	if err := config.Validate(); string(err.(ConfigurationError)) != "ClientID is invalid" {
		t.Error("Expected invalid ClientID, got ", err)
	}
}

func TestEmptyClientIDConfigValidates(t *testing.T) {
	config := NewConfig()
	config.ClientID = ""
	if err := config.Validate(); string(err.(ConfigurationError)) != "ClientID is invalid" {
		t.Error("Expected invalid ClientID, got ", err)
	}
}

// This example shows how to integrate with an existing registry as well as publishing metrics
// on the standard output
func ExampleConfig_metrics() {
	// Our application registry
	appMetricRegistry := metrics.NewRegistry()
	appGauge := metrics.GetOrRegisterGauge("m1", appMetricRegistry)
	appGauge.Update(1)

	config := NewConfig()
	// Use a prefix registry instead of the default local one
	config.MetricRegistry = metrics.NewPrefixedChildRegistry(appMetricRegistry, "sarama.")

	// Simulate a metric created by sarama without starting a broker
	saramaGauge := metrics.GetOrRegisterGauge("m2", config.MetricRegistry)
	saramaGauge.Update(2)

	metrics.WriteOnce(appMetricRegistry, os.Stdout)
	// Output:
	// gauge m1
	//   value:               1
	// gauge sarama.m2
	//   value:               2
}

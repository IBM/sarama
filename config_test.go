package sarama

import (
	"testing"

	"github.com/rcrowley/go-metrics"
)

func TestDefaultConfigValidates(t *testing.T) {
	config := NewConfig()
	if err := config.Validate(); err != nil {
		t.Error(err)
	}
	if config.MetricRegistry != metrics.DefaultRegistry {
		t.Error("Expected metrics.DefaultRegistry, got ", config.MetricRegistry)
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

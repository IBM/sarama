package sarama

import (
	"fmt"

	"github.com/rcrowley/go-metrics"
)

func getOrRegisterHistogram(name string, r metrics.Registry) metrics.Histogram {
	return r.GetOrRegister(name, func() metrics.Histogram {
		return metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015))
	}).(metrics.Histogram)
}

func getMetricNameForBroker(name string, broker *Broker) string {
	// Use broker id like the Java client as it does not contain '.' or ':' characters that
	// can be interpreted as special character by monitoring tool (e.g. Graphite)
	return fmt.Sprintf(name+"-for-broker-%d", broker.ID())
}

func getOrRegisterBrokerMeter(name string, broker *Broker, r metrics.Registry) metrics.Meter {
	return metrics.GetOrRegisterMeter(getMetricNameForBroker(name, broker), r)
}

func getOrRegisterBrokerHistogram(name string, broker *Broker, r metrics.Registry) metrics.Histogram {
	return getOrRegisterHistogram(getMetricNameForBroker(name, broker), r)
}

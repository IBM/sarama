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

func getMetricNameForBroker(name string, brokerId int32) string {
	return fmt.Sprintf(name+"-for-broker-%d", brokerId)
}

func getOrRegisterBrokerMeter(name string, brokerId int32, r metrics.Registry) metrics.Meter {
	return metrics.GetOrRegisterMeter(getMetricNameForBroker(name, brokerId), r)
}

func getOrRegisterBrokerHistogram(name string, brokerId int32, r metrics.Registry) metrics.Histogram {
	return getOrRegisterHistogram(getMetricNameForBroker(name, brokerId), r)
}

func getMetricNameForTopic(name string, topic string) string {
	return fmt.Sprintf(name+"-for-topic-%s", topic)
}

func getOrRegisterTopicMeter(name string, topic string, r metrics.Registry) metrics.Meter {
	return metrics.GetOrRegisterMeter(getMetricNameForTopic(name, topic), r)
}

func getOrRegisterTopicHistogram(name string, topic string, r metrics.Registry) metrics.Histogram {
	return getOrRegisterHistogram(getMetricNameForTopic(name, topic), r)
}

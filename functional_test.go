package sarama

import (
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	VagrantKafkaPeers     = "192.168.100.67:9091,192.168.100.67:9092,192.168.100.67:9093,192.168.100.67:9094,192.168.100.67:9095"
	VagrantZookeeperPeers = "192.168.100.67:2181,192.168.100.67:2182,192.168.100.67:2183,192.168.100.67:2184,192.168.100.67:2185"
)

var (
	kafkaIsAvailable, kafkaShouldBeAvailable bool
	kafkaBrokers                             []string
)

func init() {
	kafkaPeers := os.Getenv("KAFKA_PEERS")
	if kafkaPeers == "" {
		kafkaPeers = VagrantKafkaPeers
	}
	kafkaBrokers = strings.Split(kafkaPeers, ",")

	if c, err := net.DialTimeout("tcp", kafkaBrokers[0], 5*time.Second); err == nil {
		if err = c.Close(); err == nil {
			kafkaIsAvailable = true
		}
	}

	kafkaShouldBeAvailable = os.Getenv("CI") != ""

	if os.Getenv("DEBUG") == "true" {
		Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}
}

func checkKafkaAvailability(t *testing.T) {
	if !kafkaIsAvailable {
		if kafkaShouldBeAvailable {
			t.Fatalf("Kafka broker is not available on %s. Set KAFKA_PEERS to connect to Kafka on a different location.", kafkaBrokers[0])
		} else {
			t.Skipf("Kafka broker is not available on %s. Set KAFKA_PEERS to connect to Kafka on a different location.", kafkaBrokers[0])
		}
	}
}

func checkKafkaVersion(t *testing.T, requiredVersion string) {
	kafkaVersion := os.Getenv("KAFKA_VERSION")
	if kafkaVersion == "" {
		t.Logf("No KAFKA_VERSION set. This tests requires Kafka version %s or higher. Continuing...", requiredVersion)
	} else {
		available := parseKafkaVersion(kafkaVersion)
		required := parseKafkaVersion(requiredVersion)
		if !available.satisfies(required) {
			t.Skipf("Kafka version %s is required for this test; you have %s. Skipping...", requiredVersion, kafkaVersion)
		}
	}
}

type kafkaVersion []int

func (kv kafkaVersion) satisfies(other kafkaVersion) bool {
	var ov int
	for index, v := range kv {
		if len(other) <= index {
			ov = 0
		} else {
			ov = other[index]
		}

		if v < ov {
			return false
		}
	}
	return true
}

func parseKafkaVersion(version string) kafkaVersion {
	numbers := strings.Split(version, ".")
	result := make(kafkaVersion, 0, len(numbers))
	for _, number := range numbers {
		nr, _ := strconv.Atoi(number)
		result = append(result, nr)
	}

	return result
}

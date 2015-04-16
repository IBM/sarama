package sarama

import (
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/client"
)

const (
	VagrantToxiproxy      = "http://192.168.100.67:8474"
	VagrantKafkaPeers     = "192.168.100.67:9091,192.168.100.67:9092,192.168.100.67:9093,192.168.100.67:9094,192.168.100.67:9095"
	VagrantZookeeperPeers = "192.168.100.67:2181,192.168.100.67:2182,192.168.100.67:2183,192.168.100.67:2184,192.168.100.67:2185"
)

var (
	kafkaIsAvailable, kafkaShouldBeAvailable bool
	kafkaBrokers                             []string
	proxy                                    *toxiproxy.Client
)

func init() {
	if os.Getenv("DEBUG") == "true" {
		Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	seed := time.Now().UTC().UnixNano()
	if tmp := os.Getenv("TEST_SEED"); tmp != "" {
		seed, _ = strconv.ParseInt(tmp, 0, 64)
	}
	Logger.Println("Using random seed:", seed)
	rand.Seed(seed)

	proxyAddr := os.Getenv("TOXIPROXY_ADDR")
	if proxyAddr == "" {
		proxyAddr = VagrantToxiproxy
	}
	proxy = toxiproxy.NewClient(proxyAddr)

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
}

func checkKafkaAvailability(t testing.TB) {
	if !kafkaIsAvailable {
		if kafkaShouldBeAvailable {
			t.Fatalf("Kafka broker is not available on %s. Set KAFKA_PEERS to connect to Kafka on a different location.", kafkaBrokers[0])
		} else {
			t.Skipf("Kafka broker is not available on %s. Set KAFKA_PEERS to connect to Kafka on a different location.", kafkaBrokers[0])
		}
	}
}

func checkKafkaVersion(t testing.TB, requiredVersion string) {
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

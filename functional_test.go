//+build functional

package sarama

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/client"
)

const (
	uncomittedMsgJar = "https://github.com/FrancoisPoinsot/simplest-uncommitted-msg/releases/download/0.1/simplest-uncommitted-msg-0.1-jar-with-dependencies.jar"
)

var (
	testTopicDetails = map[string]*TopicDetail{
		"test.1": {
			NumPartitions:     1,
			ReplicationFactor: 3,
		},
		"test.4": {
			NumPartitions:     4,
			ReplicationFactor: 3,
		},
		"test.64": {
			NumPartitions:     64,
			ReplicationFactor: 3,
		},
		"uncommitted-topic-test-4": {
			NumPartitions:     1,
			ReplicationFactor: 3,
		},
	}

	FunctionalTestEnv *testEnvironment
)

func TestMain(m *testing.M) {
	// Functional tests for Sarama
	//
	// You can either set TOXIPROXY_ADDR, which points at a toxiproxy address
	// already set up with 21801-21805 bound to zookeeper and 29091-29095
	// bound to kafka. Alternatively, if TOXIPROXY_ADDR is not set, we'll try
	// and use Docker to bring up a 5-node zookeeper cluster & 5-node kafka
	// cluster, with toxiproxy configured as above.
	//
	// In either case, the following topics will be deleted (if they exist) and
	// then created/pre-seeded with data for the functional test run:
	//     * uncomitted-topic-test-4
	//     * test.1
	//     * test.4
	//     * test.64
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	ctx := context.Background()
	var env testEnvironment

	if os.Getenv("DEBUG") == "true" {
		Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	usingExisting, err := existingEnvironment(ctx, &env)
	if err != nil {
		panic(err)
	}
	if !usingExisting {
		err := prepareDockerTestEnvironment(ctx, &env)
		if err != nil {
			_ = tearDownDockerTestEnvironment(ctx, &env)
			panic(err)
		}
		defer tearDownDockerTestEnvironment(ctx, &env) // nolint:errcheck
	}
	if err := prepareTestTopics(ctx, &env); err != nil {
		panic(err)
	}
	FunctionalTestEnv = &env
	return m.Run()
}

type testEnvironment struct {
	ToxiproxyClient  *toxiproxy.Client
	Proxies          map[string]*toxiproxy.Proxy
	KafkaBrokerAddrs []string
	KafkaVersion     string
}

func prepareDockerTestEnvironment(ctx context.Context, env *testEnvironment) error {
	Logger.Println("bringing up docker-based test environment")

	// Always (try to) tear down first.
	if err := tearDownDockerTestEnvironment(ctx, env); err != nil {
		return fmt.Errorf("failed to tear down existing env: %w", err)
	}

	if version, ok := os.LookupEnv("KAFKA_VERSION"); ok {
		env.KafkaVersion = version
	} else {
		// We have cp-5.5.0 as the default in the docker-compose file, so that's kafka 2.5.0.
		env.KafkaVersion = "2.5.0"
	}

	// the mapping of confluent platform docker image versions -> kafka versions can be
	// found here: https://docs.confluent.io/current/installation/versions-interoperability.html
	var confluentPlatformVersion string
	switch env.KafkaVersion {
	case "2.6.0":
		confluentPlatformVersion = "5.5.0"
	case "2.4.1":
		confluentPlatformVersion = "5.4.2"
	default:
		return fmt.Errorf("don't know what confluent platform version to use for kafka %s", env.KafkaVersion)
	}

	c := exec.Command("docker-compose", "up", "-d")
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	c.Env = append(os.Environ(), fmt.Sprintf("CONFLUENT_PLATFORM_VERSION=%s", confluentPlatformVersion))
	err := c.Run()
	if err != nil {
		return fmt.Errorf("failed to run docker-compose to start test enviroment: %w", err)
	}

	// Set up toxiproxy Proxies
	env.ToxiproxyClient = toxiproxy.NewClient("localhost:8474")
	env.Proxies = map[string]*toxiproxy.Proxy{}
	for i := 1; i <= 5; i++ {
		proxyName := fmt.Sprintf("kafka%d", i)
		proxy, err := env.ToxiproxyClient.CreateProxy(
			proxyName,
			fmt.Sprintf("0.0.0.0:%d", 29090+i),
			fmt.Sprintf("kafka-%d:%d", i, 29090+i),
		)
		if err != nil {
			return fmt.Errorf("failed to create toxiproxy: %w", err)
		}
		env.Proxies[proxyName] = proxy
		env.KafkaBrokerAddrs = append(env.KafkaBrokerAddrs, fmt.Sprintf("127.0.0.1:%d", 29090+i))
	}

	// Wait for the kafka broker to come up
	allBrokersUp := false
	for i := 0; i < 45 && !allBrokersUp; i++ {
		Logger.Println("waiting for kafka brokers to come up")
		time.Sleep(1 * time.Second)
		config := NewConfig()
		config.Version, err = ParseKafkaVersion(env.KafkaVersion)
		if err != nil {
			return err
		}
		config.Net.DialTimeout = 1 * time.Second
		config.Net.ReadTimeout = 1 * time.Second
		config.Net.WriteTimeout = 1 * time.Second
		config.ClientID = "sarama-tests"
		brokersOk := make([]bool, len(env.KafkaBrokerAddrs))
	retryLoop:
		for j, addr := range env.KafkaBrokerAddrs {
			client, err := NewClient([]string{addr}, config)
			if err != nil {
				continue
			}
			err = client.RefreshMetadata()
			if err != nil {
				continue
			}
			brokers := client.Brokers()
			if len(brokers) < 5 {
				continue
			}
			for _, broker := range brokers {
				err := broker.Open(client.Config())
				if err != nil {
					continue retryLoop
				}
				connected, err := broker.Connected()
				if err != nil || !connected {
					continue retryLoop
				}
			}
			brokersOk[j] = true
		}
		allBrokersUp = true
		for _, u := range brokersOk {
			allBrokersUp = allBrokersUp && u
		}
	}
	if !allBrokersUp {
		return fmt.Errorf("timed out waiting for broker to come up")
	}

	return nil
}

func existingEnvironment(ctx context.Context, env *testEnvironment) (bool, error) {
	toxiproxyAddr, ok := os.LookupEnv("TOXIPROXY_ADDR")
	if !ok {
		return false, nil
	}
	toxiproxyURL, err := url.Parse(toxiproxyAddr)
	if err != nil {
		return false, fmt.Errorf("$TOXIPROXY_ADDR not parseable as url")
	}
	toxiproxyHost := toxiproxyURL.Hostname()

	env.ToxiproxyClient = toxiproxy.NewClient(toxiproxyAddr)
	for i := 1; i <= 5; i++ {
		proxyName := fmt.Sprintf("kafka%d", i)
		proxy, err := env.ToxiproxyClient.Proxy(proxyName)
		if err != nil {
			return false, fmt.Errorf("no proxy kafka%d on toxiproxy: %w", i, err)
		}
		env.Proxies[proxyName] = proxy
		// get the host:port from the proxy & toxiproxy addr, so we can do "$toxiproxy_addr:$proxy_port"
		_, proxyPort, err := net.SplitHostPort(proxy.Listen)
		if err != nil {
			return false, fmt.Errorf("proxy.Listen not a host:port combo: %w", err)
		}
		env.KafkaBrokerAddrs = append(env.KafkaBrokerAddrs, fmt.Sprintf("%s:%s", toxiproxyHost, proxyPort))
	}

	env.KafkaVersion, ok = os.LookupEnv("KAFKA_VERSION")
	if !ok {
		return false, fmt.Errorf("KAFKA_VERSION needs to be provided with TOXIPROXY_ADDR")
	}
	return true, nil
}

func tearDownDockerTestEnvironment(ctx context.Context, env *testEnvironment) error {
	c := exec.Command("docker-compose", "down", "--volumes")
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	downErr := c.Run()

	c = exec.Command("docker-compose", "rm", "-v", "--force", "--stop")
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	rmErr := c.Run()
	if downErr != nil {
		return fmt.Errorf("failed to run docker-compose to stop test enviroment: %w", downErr)
	}
	if rmErr != nil {
		return fmt.Errorf("failed to run docker-compose to rm test enviroment: %w", rmErr)
	}
	return nil
}

func prepareTestTopics(ctx context.Context, env *testEnvironment) error {
	Logger.Println("creating test topics")
	var testTopicNames []string
	for topic := range testTopicDetails {
		testTopicNames = append(testTopicNames, topic)
	}

	Logger.Println("Creating topics")
	config := NewConfig()
	config.Metadata.Retry.Max = 5
	config.Metadata.Retry.Backoff = 10 * time.Second
	config.ClientID = "sarama-tests"
	var err error
	config.Version, err = ParseKafkaVersion(env.KafkaVersion)
	if err != nil {
		return fmt.Errorf("failed to parse kafka version %s: %w", env.KafkaVersion, err)
	}

	client, err := NewClient(env.KafkaBrokerAddrs, config)
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %w", err)
	}
	defer client.Close()

	controller, err := client.Controller()
	if err != nil {
		return fmt.Errorf("failed to connect to kafka controller: %w", err)
	}
	defer controller.Close()

	// Start by deleting the test topics (if they already exist)
	deleteRes, err := controller.DeleteTopics(&DeleteTopicsRequest{
		Topics:  testTopicNames,
		Timeout: 30 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("failed to delete test topics: %w", err)
	}
	for topic, topicErr := range deleteRes.TopicErrorCodes {
		if !isTopicNotExistsErrorOrOk(topicErr) {
			return fmt.Errorf("failed to delete topic %s: %w", topic, topicErr)
		}
	}

	// wait for the topics to _actually_ be gone - the delete is not guaranteed to be processed
	// synchronously
	var topicsOk bool
	for i := 0; i < 20 && !topicsOk; i++ {
		time.Sleep(1 * time.Second)
		md, err := controller.GetMetadata(&MetadataRequest{
			Topics: testTopicNames,
		})
		if err != nil {
			return fmt.Errorf("failed to get metadata for test topics: %w", err)
		}

		topicsOk = true
		for _, topicsMd := range md.Topics {
			if !isTopicNotExistsErrorOrOk(topicsMd.Err) {
				topicsOk = false
			}
		}
	}
	if !topicsOk {
		return fmt.Errorf("timed out waiting for test topics to be gone")
	}

	// now create the topics empty
	createRes, err := controller.CreateTopics(&CreateTopicsRequest{
		TopicDetails: testTopicDetails,
		Timeout:      30 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("failed to create test topics: %w", err)
	}
	for topic, topicErr := range createRes.TopicErrors {
		if !isTopicExistsErrorOrOk(topicErr.Err) {
			return fmt.Errorf("failed to create test topic %s: %w", topic, topicErr)
		}
	}

	// This is kind of gross, but we don't actually have support for doing transactional publishing
	// with sarama, so we need to use a java-based tool to publish uncomitted messages to
	// the uncommitted-topic-test-4 topic
	jarName := filepath.Base(uncomittedMsgJar)
	if _, err := os.Stat(jarName); err != nil {
		Logger.Printf("Downloading %s\n", uncomittedMsgJar)
		req, err := http.NewRequest("GET", uncomittedMsgJar, nil)
		if err != nil {
			return fmt.Errorf("failed creating requst for uncomitted msg jar: %w", err)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed fetching the uncommitted msg jar: %w", err)
		}
		defer res.Body.Close()
		jarFile, err := os.OpenFile(jarName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("failed opening the uncomitted msg jar: %w", err)
		}
		defer jarFile.Close()

		_, err = io.Copy(jarFile, res.Body)
		if err != nil {
			return fmt.Errorf("failed writing the uncomitted msg jar: %w", err)
		}
	}

	c := exec.Command("java", "-jar", jarName, "-b", env.KafkaBrokerAddrs[0], "-c", "4")
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	err = c.Run()
	if err != nil {
		return fmt.Errorf("failed running uncomitted msg jar: %w", err)
	}
	return nil
}

func isTopicNotExistsErrorOrOk(err KError) bool {
	return err == ErrUnknownTopicOrPartition || err == ErrInvalidTopic || err == ErrNoError
}

func isTopicExistsErrorOrOk(err KError) bool {
	return err == ErrTopicAlreadyExists || err == ErrNoError
}

func checkKafkaVersion(t testing.TB, requiredVersion string) {
	kafkaVersion := FunctionalTestEnv.KafkaVersion
	if kafkaVersion == "" {
		t.Skipf("No KAFKA_VERSION set. This test requires Kafka version %s or higher. Continuing...", requiredVersion)
	} else {
		available := parseKafkaVersion(kafkaVersion)
		required := parseKafkaVersion(requiredVersion)
		if !available.satisfies(required) {
			t.Skipf("Kafka version %s is required for this test; you have %s. Skipping...", requiredVersion, kafkaVersion)
		}
	}
}

func resetProxies(t testing.TB) {
	if err := FunctionalTestEnv.ToxiproxyClient.ResetState(); err != nil {
		t.Error(err)
	}
}

func SaveProxy(t *testing.T, px string) {
	if err := FunctionalTestEnv.Proxies[px].Save(); err != nil {
		t.Fatal(err)
	}
}

func setupFunctionalTest(t testing.TB) {
	resetProxies(t)
}

func teardownFunctionalTest(t testing.TB) {
	resetProxies(t)
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
		} else if v > ov {
			return true
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

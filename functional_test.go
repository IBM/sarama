//go:build functional

package sarama

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama/internal/toxiproxy"
)

const uncommittedTopic = "uncommitted-topic-test-4"

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
		uncommittedTopic: {
			NumPartitions:     1,
			ReplicationFactor: 3,
		},
		"test.1_to_2": {
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
	//     * uncommitted-topic-test-4
	//     * test.1
	//     * test.4
	//     * test.64
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	ctx := context.Background()
	var env testEnvironment

	if os.Getenv("DEBUG") == "true" {
		Logger = log.New(os.Stderr, "[DEBUG] ", log.Lmicroseconds|log.Ltime)
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

// NewFunctionalTestConfig returns a config meant to be used by functional tests.
func NewFunctionalTestConfig() *Config {
	config := NewConfig()
	// config.Consumer.Retry.Backoff = 0
	// config.Producer.Retry.Backoff = 0
	config.Version = MinVersion
	version, err := ParseKafkaVersion(os.Getenv("KAFKA_VERSION"))
	if err != nil {
		config.Version = DefaultVersion
	} else {
		config.Version = version
	}
	return config
}

type testEnvironment struct {
	ToxiproxyClient  *toxiproxy.Client
	Proxies          map[string]*toxiproxy.Proxy
	KafkaBrokerAddrs []string
	KafkaVersion     string
}

// setupToxiProxies will configure the toxiproxy proxies with routes for the
// kafka brokers if they don't already exist
func setupToxiProxies(env *testEnvironment, endpoint string) error {
	env.ToxiproxyClient = toxiproxy.NewClient(endpoint)
	env.Proxies = map[string]*toxiproxy.Proxy{}
	env.KafkaBrokerAddrs = nil
	for i := 1; i <= 5; i++ {
		proxyName := fmt.Sprintf("kafka%d", i)
		proxy, err := env.ToxiproxyClient.Proxy(proxyName)
		if err != nil {
			proxy, err = env.ToxiproxyClient.CreateProxy(
				proxyName,
				fmt.Sprintf("0.0.0.0:%d", 29090+i),
				fmt.Sprintf("kafka-%d:%d", i, 29090+i),
			)
			if err != nil {
				return fmt.Errorf("failed to create toxiproxy: %w", err)
			}
		}
		env.Proxies[proxyName] = proxy
		env.KafkaBrokerAddrs = append(env.KafkaBrokerAddrs, fmt.Sprintf("127.0.0.1:%d", 29090+i))
	}
	return nil
}

func prepareDockerTestEnvironment(ctx context.Context, env *testEnvironment) error {
	const expectedBrokers = 5

	Logger.Println("bringing up docker-based test environment")

	// Always (try to) tear down first.
	if err := tearDownDockerTestEnvironment(ctx, env); err != nil {
		return fmt.Errorf("failed to tear down existing env: %w", err)
	}

	if version, ok := os.LookupEnv("KAFKA_VERSION"); ok {
		env.KafkaVersion = version
	} else {
		env.KafkaVersion = "3.5.1"
	}

	// docker compose v2.17.0 or newer required for `--wait-timeout` support
	c := exec.Command(
		"docker", "compose", "up", "-d", "--quiet-pull", "--timestamps", "--wait", "--wait-timeout", "600",
	)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	c.Env = append(os.Environ(), fmt.Sprintf("KAFKA_VERSION=%s", env.KafkaVersion))
	err := c.Run()
	if err != nil {
		return fmt.Errorf("failed to run docker compose to start test environment: %w", err)
	}

	if err := setupToxiProxies(env, "http://localhost:8474"); err != nil {
		return fmt.Errorf("failed to setup toxiproxies: %w", err)
	}

	dialCheck := func(addr string, timeout time.Duration) error {
		conn, err := net.DialTimeout("tcp", addr, timeout)
		if err != nil {
			return err
		}
		return conn.Close()
	}

	config := NewFunctionalTestConfig()
	config.Net.DialTimeout = 1 * time.Second
	config.Net.ReadTimeout = 1 * time.Second
	config.Net.WriteTimeout = 1 * time.Second
	config.ClientID = "sarama-tests"

	// wait for the kafka brokers to come up
	allBrokersUp := false

	Logger.Printf("waiting for kafka %s brokers to come up...\n", env.KafkaVersion)
	time.Sleep(10 * time.Second)

mainLoop:
	for i := 0; i < 30 && !allBrokersUp; i++ {
		if i > 0 {
			Logger.Printf("still waiting for kafka %s brokers to come up...\n", env.KafkaVersion)
		}
		time.Sleep(3 * time.Second)
		brokersOk := make([]bool, len(env.KafkaBrokerAddrs))

		// first check that all bootstrap brokers are TCP accessible
		for _, addr := range env.KafkaBrokerAddrs {
			if err := dialCheck(addr, time.Second); err != nil {
				continue mainLoop
			}
		}

		// now check we can bootstrap metadata from the cluster and all brokers
		// are known and accessible at their advertised address
	retryLoop:
		for j, addr := range env.KafkaBrokerAddrs {
			client, err := NewClient([]string{addr}, config)
			if err != nil {
				continue
			}
			err = client.RefreshMetadata()
			if err != nil {
				client.Close()
				continue
			}
			brokers := client.Brokers()
			if len(brokers) < expectedBrokers {
				client.Close()
				continue
			}
			for _, broker := range brokers {
				err := broker.Open(client.Config())
				if err != nil && !errors.Is(err, ErrAlreadyConnected) {
					client.Close()
					continue retryLoop
				}
				connected, err := broker.Connected()
				if err != nil || !connected {
					broker.Close()
					client.Close()
					continue retryLoop
				}
			}
			client.Close()
			brokersOk[j] = true
		}

		allBrokersUp = true
		for _, u := range brokersOk {
			allBrokersUp = allBrokersUp && u
		}
	}

	if !allBrokersUp {
		c := exec.Command("docker", "compose", "logs", "-t", "kafka-1", "kafka-2", "kafka-3", "kafka-4", "kafka-5")
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		_ = c.Run()
		return fmt.Errorf("timed out waiting for one or more broker to come up")
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
	if err := setupToxiProxies(env, toxiproxyURL.String()); err != nil {
		return false, fmt.Errorf("failed to setup toxiproxies: %w", err)
	}

	env.KafkaVersion, ok = os.LookupEnv("KAFKA_VERSION")
	if !ok {
		return false, fmt.Errorf("KAFKA_VERSION needs to be provided with TOXIPROXY_ADDR")
	}
	return true, nil
}

func tearDownDockerTestEnvironment(ctx context.Context, env *testEnvironment) error {
	c := exec.Command("docker", "compose", "down", "--volumes")
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	downErr := c.Run()

	c = exec.Command("docker", "compose", "rm", "-v", "--force", "--stop")
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	rmErr := c.Run()
	if downErr != nil {
		return fmt.Errorf("failed to run docker compose to stop test environment: %w", downErr)
	}
	if rmErr != nil {
		return fmt.Errorf("failed to run docker compose to rm test environment: %w", rmErr)
	}
	return nil
}

func startDockerTestBroker(ctx context.Context, brokerID int32) error {
	service := fmt.Sprintf("kafka-%d", brokerID)
	c := exec.Command("docker", "compose", "start", service)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("failed to run docker compose to start test broker kafka-%d: %w", brokerID, err)
	}
	return nil
}

func stopDockerTestBroker(ctx context.Context, brokerID int32) error {
	service := fmt.Sprintf("kafka-%d", brokerID)
	c := exec.Command("docker", "compose", "stop", service)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("failed to run docker compose to stop test broker kafka-%d: %w", brokerID, err)
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
	config := NewFunctionalTestConfig()
	config.Metadata.Retry.Max = 5
	config.Metadata.Retry.Backoff = 10 * time.Second
	config.ClientID = "sarama-prepareTestTopics"

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
	{
		request := NewDeleteTopicsRequest(config.Version, testTopicNames, time.Minute)
		deleteRes, err := controller.DeleteTopics(request)
		if err != nil {
			return fmt.Errorf("failed to delete test topics: %w", err)
		}
		for topic, topicErr := range deleteRes.TopicErrorCodes {
			if !isTopicNotExistsErrorOrOk(topicErr) {
				return fmt.Errorf("failed to delete topic %s: %w", topic, topicErr)
			}
		}
	}

	// wait for the topics to _actually_ be gone - the delete is not guaranteed to be processed
	// synchronously
	{
		var topicsOk bool
		request := NewMetadataRequest(config.Version, testTopicNames)
		for i := 0; i < 60 && !topicsOk; i++ {
			time.Sleep(1 * time.Second)
			md, err := controller.GetMetadata(request)
			if err != nil {
				return fmt.Errorf("failed to get metadata for test topics: %w", err)
			}

			if len(md.Topics) == len(testTopicNames) {
				topicsOk = true
				for _, topicsMd := range md.Topics {
					if !isTopicNotExistsErrorOrOk(topicsMd.Err) {
						topicsOk = false
					}
				}
			}
		}
		if !topicsOk {
			return fmt.Errorf("timed out waiting for test topics to be gone")
		}
	}

	// now create the topics empty
	{
		request := NewCreateTopicsRequest(config.Version, testTopicDetails, time.Minute)
		createRes, err := controller.CreateTopics(request)
		if err != nil {
			return fmt.Errorf("failed to create test topics: %w", err)
		}
		for topic, topicErr := range createRes.TopicErrors {
			if !isTopicExistsErrorOrOk(topicErr.Err) {
				return fmt.Errorf("failed to create test topic %s: %w", topic, topicErr)
			}
		}
	}

	// wait for the topics to _actually_ exist - the creates are not guaranteed to be processed
	// synchronously
	{
		var topicsOk bool
		request := NewMetadataRequest(config.Version, testTopicNames)
		for i := 0; i < 60 && !topicsOk; i++ {
			time.Sleep(1 * time.Second)
			md, err := controller.GetMetadata(request)
			if err != nil {
				return fmt.Errorf("failed to get metadata for test topics: %w", err)
			}

			if len(md.Topics) == len(testTopicNames) {
				topicsOk = true
				for _, topicsMd := range md.Topics {
					if topicsMd.Err != ErrNoError {
						topicsOk = false
					}
				}
			}
		}
		if !topicsOk {
			return fmt.Errorf("timed out waiting for test topics to be created")
		}
	}

	return nil
}

func isTopicNotExistsErrorOrOk(err KError) bool {
	return errors.Is(err, ErrUnknownTopicOrPartition) || errors.Is(err, ErrInvalidTopic) || errors.Is(err, ErrNoError)
}

func isTopicExistsErrorOrOk(err KError) bool {
	return errors.Is(err, ErrTopicAlreadyExists) || errors.Is(err, ErrNoError)
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
	if _, err := FunctionalTestEnv.Proxies[px].Save(); err != nil {
		t.Fatal(err)
	}
}

func setupFunctionalTest(t testing.TB) {
	resetProxies(t)
	ensureFullyReplicated(t, 60*time.Second, 5*time.Second)
}

func teardownFunctionalTest(t testing.TB) {
	resetProxies(t)
}

func ensureFullyReplicated(t testing.TB, timeout time.Duration, retry time.Duration) {
	config := NewFunctionalTestConfig()
	config.Metadata.Full = false
	config.Metadata.RefreshFrequency = 0
	config.Metadata.Retry.Max = 5
	config.Metadata.Retry.Backoff = 10 * time.Second
	config.ClientID = "sarama-ensureFullyReplicated"
	config.ApiVersionsRequest = false

	var testTopicNames []string
	for topic := range testTopicDetails {
		testTopicNames = append(testTopicNames, topic)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	tick := time.NewTicker(retry)
	defer tick.Stop()

	for {
		resp, err := func() (*MetadataResponse, error) {
			client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
			if err != nil {
				return nil, fmt.Errorf("failed to connect to kafka: %w", err)
			}
			defer client.Close()
			broker := client.LeastLoadedBroker()
			defer broker.Close()
			request := NewMetadataRequest(config.Version, testTopicNames)
			return broker.GetMetadata(request)
		}()
		if err != nil {
			Logger.Printf("failed to get metadata during test setup: %v\n", err)
		} else {
			ok := true
			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					if len(partition.Isr) != 3 {
						ok = false
						Logger.Printf("topic %s/%d is not fully-replicated Isr=%v Offline=%v\n", topic.Name, partition.ID, partition.Isr, partition.OfflineReplicas)
					}
				}
			}
			if ok {
				return
			}
		}
		select {
		case <-timer.C:
			t.Fatalf("timeout waiting for test topics to be fully replicated")
		case <-tick.C:
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

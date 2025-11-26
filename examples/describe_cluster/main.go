package main

import (
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/IBM/sarama"
)

var (
	brokers     = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "Kafka bootstrap brokers to connect to, as a comma separated list")
	kafkaVer    = flag.String("version", sarama.DefaultVersion.String(), "Kafka cluster version")
	clientID    = flag.String("client-id", "describe-cluster-example", "Client identifier")
	endpoint    = flag.String("endpoint-type", "brokers", "DescribeCluster endpoint type to request: brokers or controllers")
	includeOps  = flag.Bool("include-cluster-ops", false, "Include cluster authorized operations in the response")
	includeFsvc = flag.Bool("include-fenced-brokers", true, "Include fenced brokers when supported (Kafka >= 4.0)")
	verbose     = flag.Bool("verbose", false, "Enable Sarama debug logging")

	saslMechanism = flag.String("sasl-mechanism", "", "SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)")
	saslUser      = flag.String("sasl-user", "", "SASL username")
	saslPassword  = flag.String("sasl-password", "", "SASL password")
	saslAuthzID   = flag.String("sasl-authzid", "", "Optional SASL authorization identity (authzid)")
)

func main() {
	flag.Parse()

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(*kafkaVer)
	if err != nil {
		log.Fatalf("invalid Kafka version %q: %v", *kafkaVer, err)
	}

	config := sarama.NewConfig()
	config.Version = version
	config.ClientID = *clientID

	if err := configureSASL(config); err != nil {
		log.Fatalf("invalid SASL configuration: %v", err)
	}

	adminClient, err := sarama.NewClient(strings.Split(*brokers, ","), config)
	if err != nil {
		log.Fatalf("failed to create Sarama client: %v", err)
	}
	defer func() {
		if err := adminClient.Close(); err != nil {
			log.Printf("failed to close client: %v", err)
		}
	}()

	controller, err := adminClient.Controller()
	if err != nil {
		log.Fatalf("failed to get controller: %v", err)
	}
	defer func() {
		if err := controller.Close(); err != nil && !errors.Is(err, sarama.ErrNotConnected) {
			log.Printf("failed to close controller connection: %v", err)
		}
	}()

	if err := controller.Open(adminClient.Config()); err != nil && !errors.Is(err, sarama.ErrAlreadyConnected) {
		log.Fatalf("failed to open controller connection: %v", err)
	}

	request := sarama.NewDescribeClusterRequest(config.Version)
	request.IncludeClusterAuthorizedOperations = *includeOps

	if request.Version >= 1 {
		endpointType, err := parseEndpointType(*endpoint)
		if err != nil {
			log.Fatal(err)
		}
		request.EndpointType = endpointType
	} else if *endpoint != "brokers" {
		log.Printf("endpoint type flag ignored on Kafka versions older than 3.7 (request v%d)", request.Version)
	}

	if request.Version >= 2 {
		request.IncludeFencedBrokers = *includeFsvc
	} else if !*includeFsvc {
		log.Printf("include-fenced-brokers flag ignored on Kafka versions older than 4.0 (request v%d)", request.Version)
	}

	response, err := controller.DescribeCluster(request)
	if err != nil {
		log.Fatalf("describe cluster call failed: %v", err)
	}
	if response.Err != sarama.ErrNoError {
		if response.ErrorMessage != nil && *response.ErrorMessage != "" {
			log.Fatalf("describe cluster returned %s: %s", response.Err, *response.ErrorMessage)
		}
		log.Fatalf("describe cluster returned %s", response.Err)
	}

	printClusterInfo(response)
}

func parseEndpointType(value string) (int8, error) {
	switch strings.ToLower(value) {
	case "", "broker", "brokers":
		return sarama.DescribeClusterEndpointTypeBrokers, nil
	case "controller", "controllers":
		return sarama.DescribeClusterEndpointTypeControllers, nil
	default:
		return 0, fmt.Errorf("unsupported endpoint type %q (use brokers or controllers)", value)
	}
}

func endpointTypeLabel(value int8) string {
	switch value {
	case sarama.DescribeClusterEndpointTypeBrokers:
		return "brokers"
	case sarama.DescribeClusterEndpointTypeControllers:
		return "controllers"
	default:
		return fmt.Sprintf("unknown(%d)", value)
	}
}

func printClusterInfo(resp *sarama.DescribeClusterResponse) {
	fmt.Printf("Cluster ID: %s\n", resp.ClusterID)
	fmt.Printf("Controller ID: %d\n", resp.ControllerID)
	if resp.Version >= 1 {
		fmt.Printf("Endpoint type: %s\n", endpointTypeLabel(resp.EndpointType))
	}
	if resp.ClusterAuthorizedOperations != 0 {
		fmt.Printf("Cluster operations: %s\n", strings.Join(formatAuthorizedOperations(resp.ClusterAuthorizedOperations), ", "))
	} else {
		fmt.Println("Cluster operations: not requested")
	}

	if len(resp.Brokers) == 0 {
		fmt.Println("No brokers returned.")
		return
	}

	fmt.Println("\nBrokers:")
	for _, broker := range resp.Brokers {
		rack := "<none>"
		if broker.Rack != nil && *broker.Rack != "" {
			rack = *broker.Rack
		}
		fmt.Printf("  - id=%d host=%s port=%d rack=%s", broker.BrokerID, broker.Host, broker.Port, rack)
		if resp.Version >= 2 {
			fmt.Printf(" fenced=%t", broker.IsFenced)
		}
		fmt.Println()
	}
}

func formatAuthorizedOperations(mask int32) []string {
	if mask == 0 {
		return nil
	}
	if mask < 0 {
		return []string{"All"}
	}

	var operations []string
	for op := sarama.AclOperationRead; op <= sarama.AclOperationIdempotentWrite; op++ {
		if mask&(1<<uint(op)) == 0 {
			continue
		}

		opCopy := op
		operations = append(operations, opCopy.String())
	}

	if len(operations) == 0 {
		return []string{fmt.Sprintf("raw:%d", mask)}
	}
	return operations
}

func configureSASL(config *sarama.Config) error {
	mech := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(*saslMechanism), "_", "-"))
	if mech == "" {
		if *saslUser != "" || *saslPassword != "" {
			return errors.New("sasl-mechanism must be provided when username or password is set")
		}
		return nil
	}

	if *saslUser == "" || *saslPassword == "" {
		return errors.New("sasl-user and sasl-password must be provided when SASL is enabled")
	}

	config.Net.SASL.Enable = true
	config.Net.SASL.User = *saslUser
	config.Net.SASL.Password = *saslPassword
	config.Net.SASL.AuthIdentity = *saslAuthzID

	switch mech {
	case "PLAIN", "PLAINtext":
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	case "SCRAM-SHA-256":
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: sha256.New}
		}
	case "SCRAM-SHA-512":
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: sha512.New}
		}
	default:
		return fmt.Errorf("unsupported SASL mechanism %q", mech)
	}

	return nil
}

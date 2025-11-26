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
	brokers  = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "Kafka bootstrap brokers to connect to, as a comma separated list")
	kafkaVer = flag.String("version", sarama.DefaultVersion.String(), "Kafka cluster version")
	clientID = flag.String("client-id", "describe-cluster-example", "Client identifier")
	verbose  = flag.Bool("verbose", false, "Enable Sarama debug logging")

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

	brokerList := parseBrokerList(*brokers)
	if len(brokerList) == 0 {
		log.Fatalf("no valid broker addresses provided in %q", *brokers)
	}

	admin, err := sarama.NewClusterAdmin(brokerList, config)
	if err != nil {
		log.Fatalf("failed to create cluster admin: %v", err)
	}
	defer func() {
		if err := admin.Close(); err != nil {
			log.Printf("failed to close cluster admin: %v", err)
		}
	}()

	brokers, controllerID, err := admin.DescribeCluster()
	if err != nil {
		log.Fatalf("describe cluster call failed: %v", err)
	}

	printClusterInfo(brokers, controllerID)
}

func parseBrokerList(list string) []string {
	parts := strings.Split(list, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		result = append(result, trimmed)
	}
	return result
}

func printClusterInfo(brokers []*sarama.Broker, controllerID int32) {
	fmt.Printf("Controller ID: %d\n", controllerID)
	if len(brokers) == 0 {
		fmt.Println("No brokers returned.")
		return
	}

	fmt.Println("\nBrokers:")
	for _, broker := range brokers {
		rack := broker.Rack()
		if rack == "" {
			rack = "<none>"
		}
		fmt.Printf("  - id=%d addr=%s rack=%s\n", broker.ID(), broker.Addr(), rack)
	}
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

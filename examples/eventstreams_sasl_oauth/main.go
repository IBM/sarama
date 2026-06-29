package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/IBM/sarama"
)

func init() {
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
}

var (
	brokers = flag.String("brokers", os.Getenv("KAFKA_BROKERS"),
		"Comma separated list of Event Streams bootstrap brokers (host:port)")
	apiKey = flag.String("apikey", os.Getenv("APIKEY"),
		"IBM Cloud API key used for the IAM apikey->token exchange")
	tokenEndpoint = flag.String("token-endpoint", "https://iam.cloud.ibm.com/identity/token",
		"IAM token endpoint URL")
	version = flag.String("version", sarama.DefaultVersion.String(), "Kafka cluster version")
)

func main() {
	flag.Parse()

	if *brokers == "" {
		log.Fatalln("at least one broker is required (set -brokers or KAFKA_BROKERS)")
	}
	if *apiKey == "" {
		log.Fatalln("an IBM Cloud API key is required (set -apikey or APIKEY)")
	}

	kafkaVersion, err := sarama.ParseKafkaVersion(*version)
	if err != nil {
		log.Fatalf("error parsing Kafka version: %v", err)
	}

	conf := sarama.NewConfig()
	conf.Version = kafkaVersion
	conf.ClientID = "oauth_eventstreams"

	// Event Streams requires security.protocol=SASL_SSL ...
	conf.Net.TLS.Enable = true
	conf.Net.TLS.Config = &tls.Config{} // IAM/Event Streams use publicly trusted certs

	// ... with sasl.mechanism=OAUTHBEARER. Sarama drives the OAUTHBEARER
	// exchange through an AccessTokenProvider, which here performs the IBM
	// IAM apikey -> bearer token exchange (the Go equivalent of the Java
	// IAMOAuthBearerLoginCallbackHandler).
	conf.Net.SASL.Enable = true
	conf.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	conf.Net.SASL.TokenProvider = newIAMTokenProvider(*apiKey, *tokenEndpoint)

	admin, err := sarama.NewClusterAdmin(strings.Split(*brokers, ","), conf)
	if err != nil {
		log.Fatalf("failed to create cluster admin: %v", err)
	}
	defer func() { _ = admin.Close() }()

	topics, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("failed to list topics: %v", err)
	}

	names := make([]string, 0, len(topics))
	for name := range topics {
		names = append(names, name)
	}
	sort.Strings(names)

	fmt.Printf("Found %d topic(s):\n", len(names))
	for _, name := range names {
		t := topics[name]
		fmt.Printf("  %s (partitions=%d, replication-factor=%d)\n",
			name, t.NumPartitions, t.ReplicationFactor)
	}
}

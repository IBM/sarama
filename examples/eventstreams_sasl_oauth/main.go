package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

func init() {
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	// DebugLogger emits the SASL session lifetime and the re-authentication
	// schedule the broker reports when connections.max.reauth.ms is set, e.g.
	// "Session expiration in N ms and session re-authentication on or after M ms".
	sarama.DebugLogger = log.New(os.Stdout, "[Sarama][DEBUG] ", log.LstdFlags)
}

var (
	brokers = flag.String("brokers", os.Getenv("KAFKA_BROKERS"),
		"Comma separated list of Event Streams bootstrap brokers (host:port)")
	apiKey = flag.String("apikey", os.Getenv("APIKEY"),
		"IBM Cloud API key used for the IAM apikey->token exchange")
	tokenEndpoint = flag.String("token-endpoint", "https://iam.cloud.ibm.com/identity/token",
		"IAM token endpoint URL")
	version = flag.String("version", sarama.DefaultVersion.String(), "Kafka cluster version")
	interval = flag.Duration("interval", 30*time.Second,
		"How often to send a metadata request on the single long-lived connection. "+
			"Must be short enough that the broker does not close the connection as idle.")
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

	// To observe SASL re-authentication (KIP-368) we need a *single* connection
	// that stays open and busy long enough to cross the broker's re-auth
	// threshold (Sarama re-authenticates at ~85-95% of the session lifetime the
	// broker reports via connections.max.reauth.ms).
	//
	// A ClusterAdmin fans requests across every broker, so each individual
	// connection sits idle and gets reaped by the broker/load balancer long
	// before that threshold. Instead we open one raw broker connection and poke
	// it with a metadata request every -interval: the connection stays alive,
	// and once the threshold passes the next request triggers an in-band
	// re-authentication on that same connection (no reconnect), calling the
	// token provider again.
	addr := strings.Split(*brokers, ",")[0]
	broker, err := openBroker(addr, conf)
	if err != nil {
		log.Fatalf("failed to connect to broker %s: %v", addr, err)
	}
	defer func() { _ = broker.Close() }()
	connectedAt := time.Now()
	log.Printf("connected to %s; sending a metadata request every %s", addr, *interval)
	log.Println("re-authentication happens in-band on this connection: watch for a [token] line " +
		"that is NOT preceded by an ApiVersionsRequest/\"Connected to broker\" handshake")

	metaReq := sarama.NewMetadataRequest(kafkaVersion, nil) // nil topics => all topics

	for {
		log.Printf("--- %s (connection age %s) ---",
			time.Now().Format(time.RFC3339), time.Since(connectedAt).Round(time.Second))

		resp, err := broker.GetMetadata(metaReq)
		if err != nil {
			// The connection was lost (e.g. closed as idle). Reconnecting is a
			// fresh authentication, not a re-authentication, and resets the
			// session-lifetime clock.
			log.Printf("metadata request failed (%v); reconnecting...", err)
			_ = broker.Close()
			broker, err = openBroker(addr, conf)
			if err != nil {
				log.Printf("reconnect to %s failed: %v", addr, err)
				time.Sleep(*interval)
				continue
			}
			connectedAt = time.Now()
			resp, err = broker.GetMetadata(metaReq)
			if err != nil {
				log.Printf("metadata request failed after reconnect: %v", err)
				time.Sleep(*interval)
				continue
			}
		}

		printTopics(resp)
		log.Printf("sleeping for %s...", *interval)
		time.Sleep(*interval)
	}
}

// openBroker opens a single broker connection and waits for the asynchronous
// connect (including the SASL handshake) to complete.
func openBroker(addr string, conf *sarama.Config) (*sarama.Broker, error) {
	broker := sarama.NewBroker(addr)
	if err := broker.Open(conf); err != nil {
		return nil, err
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		connected, err := broker.Connected()
		if connected {
			return broker, nil
		}
		if err != nil {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out waiting for connection to %s", addr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func printTopics(resp *sarama.MetadataResponse) {
	names := make([]string, 0, len(resp.Topics))
	byName := make(map[string]*sarama.TopicMetadata, len(resp.Topics))
	for _, t := range resp.Topics {
		names = append(names, t.Name)
		byName[t.Name] = t
	}
	sort.Strings(names)

	log.Printf("Found %d topic(s):", len(names))
	for _, name := range names {
		t := byName[name]
		replication := 0
		if len(t.Partitions) > 0 {
			replication = len(t.Partitions[0].Replicas)
		}
		log.Printf("  %s (partitions=%d, replication-factor=%d)",
			name, len(t.Partitions), replication)
	}
}

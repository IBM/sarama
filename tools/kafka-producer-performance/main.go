package main

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	gosync "sync"
	"time"

	metrics "github.com/rcrowley/go-metrics"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/tools/tls"
)

var (
	sync = flag.Bool(
		"sync",
		false,
		"Use a synchronous producer.",
	)
	messageLoad = flag.Int(
		"message-load",
		0,
		"REQUIRED: The number of messages to produce to -topic.",
	)
	messageSize = flag.Int(
		"message-size",
		0,
		"REQUIRED: The approximate size (in bytes) of each message to produce to -topic.",
	)
	brokers = flag.String(
		"brokers",
		"",
		"REQUIRED: A comma separated list of broker addresses.",
	)
	securityProtocol = flag.String(
		"security-protocol",
		"PLAINTEXT",
		"The name of the security protocol to talk to Kafka (PLAINTEXT, SSL) (default: PLAINTEXT).",
	)
	tlsRootCACerts = flag.String(
		"tls-ca-certs",
		"",
		"The path to a file that contains a set of root certificate authorities in PEM format "+
			"to trust when verifying broker certificates when -security-protocol=SSL "+
			"(leave empty to use the host's root CA set).",
	)
	tlsClientCert = flag.String(
		"tls-client-cert",
		"",
		"The path to a file that contains the client certificate to send to the broker "+
			"in PEM format if client authentication is required when -security-protocol=SSL "+
			"(leave empty to disable client authentication).",
	)
	tlsClientKey = flag.String(
		"tls-client-key",
		"",
		"The path to a file that contains the client private key linked to the client certificate "+
			"in PEM format when -security-protocol=SSL (REQUIRED if tls-client-cert is provided).",
	)
	topic = flag.String(
		"topic",
		"",
		"REQUIRED: The topic to run the performance test on.",
	)
	partition = flag.Int(
		"partition",
		-1,
		"The partition of -topic to run the performance test on.",
	)
	throughput = flag.Int(
		"throughput",
		0,
		"The maximum number of messages to send per second (0 for no limit).",
	)
	maxOpenRequests = flag.Int(
		"max-open-requests",
		5,
		"The maximum number of unacknowledged requests the client will send on a single connection before blocking (default: 5).",
	)
	maxMessageBytes = flag.Int(
		"max-message-bytes",
		1000000,
		"The max permitted size of a message.",
	)
	requiredAcks = flag.Int(
		"required-acks",
		1,
		"The required number of acks needed from the broker (-1: all, 0: none, 1: local).",
	)
	timeout = flag.Duration(
		"timeout",
		10*time.Second,
		"The duration the producer will wait to receive -required-acks.",
	)
	partitioner = flag.String(
		"partitioner",
		"roundrobin",
		"The partitioning scheme to use (hash, manual, random, roundrobin).",
	)
	compression = flag.String(
		"compression",
		"none",
		"The compression method to use (none, gzip, snappy, lz4).",
	)
	flushFrequency = flag.Duration(
		"flush-frequency",
		0,
		"The best-effort frequency of flushes.",
	)
	flushBytes = flag.Int(
		"flush-bytes",
		0,
		"The best-effort number of bytes needed to trigger a flush.",
	)
	flushMessages = flag.Int(
		"flush-messages",
		0,
		"The best-effort number of messages needed to trigger a flush.",
	)
	flushMaxMessages = flag.Int(
		"flush-max-messages",
		0,
		"The maximum number of messages the producer will send in a single request.",
	)
	clientID = flag.String(
		"client-id",
		"sarama",
		"The client ID sent with every request to the brokers.",
	)
	channelBufferSize = flag.Int(
		"channel-buffer-size",
		256,
		"The number of events to buffer in internal and external channels.",
	)
	routines = flag.Int(
		"routines",
		1,
		"The number of routines to send the messages from (-sync only).",
	)
	version = flag.String(
		"version",
		"0.8.2.0",
		"The assumed version of Kafka.",
	)
	verbose = flag.Bool(
		"verbose",
		false,
		"Turn on sarama logging to stderr",
	)
)

func parseCompression(scheme string) sarama.CompressionCodec {
	switch scheme {
	case "none":
		return sarama.CompressionNone
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	case "lz4":
		return sarama.CompressionLZ4
	default:
		printUsageErrorAndExit(fmt.Sprintf("Unknown -compression: %s", scheme))
	}
	panic("should not happen")
}

func parsePartitioner(scheme string, partition int) sarama.PartitionerConstructor {
	if partition < 0 && scheme == "manual" {
		printUsageErrorAndExit("-partition must not be -1 for -partitioning=manual")
	}
	switch scheme {
	case "manual":
		return sarama.NewManualPartitioner
	case "hash":
		return sarama.NewHashPartitioner
	case "random":
		return sarama.NewRandomPartitioner
	case "roundrobin":
		return sarama.NewRoundRobinPartitioner
	default:
		printUsageErrorAndExit(fmt.Sprintf("Unknown -partitioning: %s", scheme))
	}
	panic("should not happen")
}

func parseVersion(version string) sarama.KafkaVersion {
	result, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		printUsageErrorAndExit(fmt.Sprintf("unknown -version: %s", version))
	}
	return result
}

func generateMessages(topic string, partition, messageLoad, messageSize int) []*sarama.ProducerMessage {
	messages := make([]*sarama.ProducerMessage, messageLoad)
	for i := 0; i < messageLoad; i++ {
		payload := make([]byte, messageSize)
		if _, err := rand.Read(payload); err != nil {
			printErrorAndExit(69, "Failed to generate message payload: %s", err)
		}
		messages[i] = &sarama.ProducerMessage{
			Topic:     topic,
			Partition: int32(partition),
			Value:     sarama.ByteEncoder(payload),
		}
	}
	return messages
}

func main() {
	flag.Parse()

	if *brokers == "" {
		printUsageErrorAndExit("-brokers is required")
	}
	if *topic == "" {
		printUsageErrorAndExit("-topic is required")
	}
	if *messageLoad <= 0 {
		printUsageErrorAndExit("-message-load must be greater than 0")
	}
	if *messageSize <= 0 {
		printUsageErrorAndExit("-message-size must be greater than 0")
	}
	if *routines < 1 || *routines > *messageLoad {
		printUsageErrorAndExit("-routines must be greater than 0 and less than or equal to -message-load")
	}
	if *securityProtocol != "PLAINTEXT" && *securityProtocol != "SSL" {
		printUsageErrorAndExit(fmt.Sprintf("-security-protocol %q is not supported", *securityProtocol))
	}
	if *verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	config := sarama.NewConfig()

	config.Net.MaxOpenRequests = *maxOpenRequests
	config.Producer.MaxMessageBytes = *maxMessageBytes
	config.Producer.RequiredAcks = sarama.RequiredAcks(*requiredAcks)
	config.Producer.Timeout = *timeout
	config.Producer.Partitioner = parsePartitioner(*partitioner, *partition)
	config.Producer.Compression = parseCompression(*compression)
	config.Producer.Flush.Frequency = *flushFrequency
	config.Producer.Flush.Bytes = *flushBytes
	config.Producer.Flush.Messages = *flushMessages
	config.Producer.Flush.MaxMessages = *flushMaxMessages
	config.Producer.Return.Successes = true
	config.ClientID = *clientID
	config.ChannelBufferSize = *channelBufferSize
	config.Version = parseVersion(*version)

	if *securityProtocol == "SSL" {
		tlsConfig, err := tls.NewConfig(*tlsClientCert, *tlsClientKey)
		if err != nil {
			printErrorAndExit(69, "failed to load client certificate from: %s and private key from: %s: %v",
				*tlsClientCert, *tlsClientKey, err)
		}

		if *tlsRootCACerts != "" {
			rootCAsBytes, err := ioutil.ReadFile(*tlsRootCACerts)
			if err != nil {
				printErrorAndExit(69, "failed to read root CA certificates: %v", err)
			}
			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM(rootCAsBytes) {
				printErrorAndExit(69, "failed to load root CA certificates from file: %s", *tlsRootCACerts)
			}
			// Use specific root CA set vs the host's set
			tlsConfig.RootCAs = certPool
		}

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}

	if err := config.Validate(); err != nil {
		printErrorAndExit(69, "Invalid configuration: %s", err)
	}

	// Print out metrics periodically.
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context) {
		defer close(done)
		t := time.Tick(5 * time.Second)
		for {
			select {
			case <-t:
				printMetrics(os.Stdout, config.MetricRegistry)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	brokers := strings.Split(*brokers, ",")
	if *sync {
		runSyncProducer(*topic, *partition, *messageLoad, *messageSize, *routines,
			config, brokers, *throughput)
	} else {
		runAsyncProducer(*topic, *partition, *messageLoad, *messageSize,
			config, brokers, *throughput)
	}

	cancel()
	<-done

	// Print final metrics.
	printMetrics(os.Stdout, config.MetricRegistry)
}

func runAsyncProducer(topic string, partition, messageLoad, messageSize int,
	config *sarama.Config, brokers []string, throughput int) {
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		printErrorAndExit(69, "Failed to create producer: %s", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			printErrorAndExit(69, "Failed to close producer: %s", err)
		}
	}()

	messages := generateMessages(topic, partition, messageLoad, messageSize)

	messagesDone := make(chan struct{})
	go func() {
		for i := 0; i < messageLoad; i++ {
			select {
			case <-producer.Successes():
			case err = <-producer.Errors():
				printErrorAndExit(69, "%s", err)
			}
		}
		messagesDone <- struct{}{}
	}()

	if throughput > 0 {
		ticker := time.NewTicker(time.Second)
		for _, message := range messages {
			for i := 0; i < throughput; i++ {
				producer.Input() <- message
			}
			<-ticker.C
		}
		ticker.Stop()
	} else {
		for _, message := range messages {
			producer.Input() <- message
		}
	}

	<-messagesDone
	close(messagesDone)
}

func runSyncProducer(topic string, partition, messageLoad, messageSize, routines int,
	config *sarama.Config, brokers []string, throughput int) {
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		printErrorAndExit(69, "Failed to create producer: %s", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			printErrorAndExit(69, "Failed to close producer: %s", err)
		}
	}()

	messages := make([][]*sarama.ProducerMessage, routines)
	for i := 0; i < routines; i++ {
		if i == routines-1 {
			messages[i] = generateMessages(topic, partition, messageLoad/routines+messageLoad%routines, messageSize)
		} else {
			messages[i] = generateMessages(topic, partition, messageLoad/routines, messageSize)
		}
	}

	var wg gosync.WaitGroup
	if throughput > 0 {
		for _, messages := range messages {
			messages := messages
			wg.Add(1)
			go func() {
				ticker := time.NewTicker(time.Second)
				for _, message := range messages {
					for i := 0; i < throughput; i++ {
						_, _, err = producer.SendMessage(message)
						if err != nil {
							printErrorAndExit(69, "Failed to send message: %s", err)
						}
					}
					<-ticker.C
				}
				ticker.Stop()
				wg.Done()
			}()
		}
	} else {
		for _, messages := range messages {
			messages := messages
			wg.Add(1)
			go func() {
				for _, message := range messages {
					_, _, err = producer.SendMessage(message)
					if err != nil {
						printErrorAndExit(69, "Failed to send message: %s", err)
					}
				}
				wg.Done()
			}()
		}
	}
	wg.Wait()
}

func printMetrics(w io.Writer, r metrics.Registry) {
	recordSendRateMetric := r.Get("record-send-rate")
	requestLatencyMetric := r.Get("request-latency-in-ms")
	outgoingByteRateMetric := r.Get("outgoing-byte-rate")
	requestsInFlightMetric := r.Get("requests-in-flight")

	if recordSendRateMetric == nil || requestLatencyMetric == nil || outgoingByteRateMetric == nil ||
		requestsInFlightMetric == nil {
		return
	}
	recordSendRate := recordSendRateMetric.(metrics.Meter).Snapshot()
	requestLatency := requestLatencyMetric.(metrics.Histogram).Snapshot()
	requestLatencyPercentiles := requestLatency.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
	outgoingByteRate := outgoingByteRateMetric.(metrics.Meter).Snapshot()
	requestsInFlight := requestsInFlightMetric.(metrics.Counter).Count()
	fmt.Fprintf(w, "%d records sent, %.1f records/sec (%.2f MiB/sec ingress, %.2f MiB/sec egress), "+
		"%.1f ms avg latency, %.1f ms stddev, %.1f ms 50th, %.1f ms 75th, "+
		"%.1f ms 95th, %.1f ms 99th, %.1f ms 99.9th, %d total req. in flight\n",
		recordSendRate.Count(),
		recordSendRate.RateMean(),
		recordSendRate.RateMean()*float64(*messageSize)/1024/1024,
		outgoingByteRate.RateMean()/1024/1024,
		requestLatency.Mean(),
		requestLatency.StdDev(),
		requestLatencyPercentiles[0],
		requestLatencyPercentiles[1],
		requestLatencyPercentiles[2],
		requestLatencyPercentiles[3],
		requestLatencyPercentiles[4],
		requestsInFlight,
	)
}

func printUsageErrorAndExit(message string) {
	fmt.Fprintln(os.Stderr, "ERROR:", message)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

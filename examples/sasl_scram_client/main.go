package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

func init() {
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
}

var (
	brokers   = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	userName  = flag.String("username", "", "The SASL username")
	passwd    = flag.String("passwd", "", "The SASL password")
	algorithm = flag.String("algorithm", "", "The SASL SCRAM SHA algorithm sha256 or sha512 as mechanism")
	topic     = flag.String("topic", "default_topic", "The Kafka topic to use")
	certFile  = flag.String("certificate", "", "The optional certificate file for client authentication")
	keyFile   = flag.String("key", "", "The optional key file for client authentication")
	caFile    = flag.String("ca", "", "The optional certificate authority file for TLS client authentication")
	verifySSL = flag.Bool("verify", false, "Optional verify ssl certificates chain")
	useTLS    = flag.Bool("tls", false, "Use TLS to communicate with the cluster")

	logger = log.New(os.Stdout, "[Producer] ", log.LstdFlags)
)

func createTLSConfiguration() (t *tls.Config) {
	t = &tls.Config{
		InsecureSkipVerify: *verifySSL,
	}
	if *certFile != "" && *keyFile != "" && *caFile != "" {
		cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := ioutil.ReadFile(*caFile)
		if err != nil {
			log.Fatal(err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: *verifySSL,
		}
	}
	return t
}

func main() {
	flag.Parse()

	if *brokers == "" {
		log.Fatalln("at least one brocker is required")
	}

	if *userName == "" {
		log.Fatalln("SASL username is required")
	}

	if *passwd == "" {
		log.Fatalln("SASL password is required")
	}

	conf := sarama.NewConfig()
	conf.Producer.Retry.Max = 1
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Successes = true
	conf.Metadata.Full = true
	conf.Version = sarama.V0_10_0_0
	conf.ClientID = "sasl_scram_client"
	conf.Metadata.Full = true
	conf.Net.SASL.Enable = true
	conf.Net.SASL.User = *userName
	conf.Net.SASL.Password = *passwd
	conf.Net.SASL.Handshake = true
	if *algorithm == "sha512" {
		conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
		conf.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
	} else if *algorithm == "sha256" {
		conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		conf.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)

	} else {
		log.Fatalf("invalid SHA algorithm \"%s\": can be either \"sha256\" or \"sha512\"", *algorithm)
	}

	if *useTLS {
		conf.Net.TLS.Enable = true
		conf.Net.TLS.Config = createTLSConfiguration()
	}

	syncProcuder, err := sarama.NewSyncProducer(strings.Split(*brokers, ","), conf)
	if err != nil {
		logger.Fatalln("failed to create producer: ", err)
	}
	partition, offset, err := syncProcuder.SendMessage(&sarama.ProducerMessage{
		Topic: *topic,
		Value: sarama.StringEncoder("test_message"),
	})
	if err != nil {
		logger.Fatalln("failed to send message to ", *topic, err)
	}
	logger.Printf("wrote message at partition: %d, offset: %d", partition, offset)
	_ = syncProcuder.Close()
	logger.Println("Bye now !")
}

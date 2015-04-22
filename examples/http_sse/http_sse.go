package main

import (
	"github.com/Shopify/sarama"

	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const (
	templateFilename = "sse_template.html"
)

var (
	addr    = flag.String("addr", ":8080", "The address to bind to")
	brokers = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	verbose = flag.Bool("verbose", false, "Turn on Sarama logging")

	pathMatcher = regexp.MustCompile("\\A/([\\w\\.-]+)/(\\d+)(?:\\.(\\w+))?")

	sseTemplate *template.Template
)

func init() {
	if data, err := ioutil.ReadFile(templateFilename); err != nil {
		panic(err)
	} else {
		sseTemplate = template.Must(template.New("sse_page").Parse(string(data)))
	}
}

func main() {
	flag.Parse()

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	config := sarama.NewConfig()
	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		log.Fatal("Failed to start Kafka client", err)
	}

	server := &SSEServer{
		client: client,
	}

	log.Fatal(server.Run(*addr))
}

type ConsumerInstatiator interface {
}

type SSEServer struct {
	client sarama.Client
}

func (s *SSEServer) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		matches := pathMatcher.FindStringSubmatch(r.URL.Path)
		if len(matches) != 4 {
			http.NotFound(w, r)
			return
		}

		topic := matches[1]
		partition, err := strconv.Atoi(matches[2])
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		switch matches[3] {
		case "sse":
			var offset int64

			// Clients automatically try to reconnect if the connection is servered.
			// When this happens the last-event-id header will be set, indicating the
			// last message it received successfully. If it is set, we start with the
			// offset, if not, we will just request new messages instead.

			lastEventId := r.Header.Get("last-event-id")
			offset, err = strconv.ParseInt(lastEventId, 10, 64)
			if err != nil {
				offset = sarama.OffsetNewest
				log.Printf("Starting new SSE stream for %s/%d...\n", topic, partition)
			} else {
				offset++
				log.Printf("Continuing SSE stream for %s/%d at offset %d...\n", topic, partition, offset)
			}

			s.Stream(w, topic, int32(partition), offset)
			log.Printf("SSE stream for %s/%d closed.\n", topic, partition)

		case "html":
			s.Template(w, topic, int32(partition))
		}
	})
}

func kafkaMessageToSSEMessage(msg *sarama.ConsumerMessage) string {
	return fmt.Sprintf(
		"id: %d\ndata: %s\n\n",
		msg.Offset,
		strings.Replace(string(msg.Value), "\n", "\ndata: ", -1),
	)
}

func (s *SSEServer) Template(w http.ResponseWriter, topic string, partition int32) {
	log.Printf("Serving SSE page for %s/%d...\n", topic, partition)

	type topicPartition struct {
		Topic     string
		Partition int32
	}

	w.Header().Set("Content-Type", "text/html")
	if err := sseTemplate.ExecuteTemplate(w, "sse_page", topicPartition{topic, partition}); err != nil {
		log.Println("Failed to compile SSE page:", err)
	}
}

func (s *SSEServer) Stream(w http.ResponseWriter, topic string, partition int32, offset int64) {
	// We have to create a separate Consumer, because a consumer can only consume every
	// topic/partition once simultaneously.
	consumer, err := sarama.NewConsumerFromClient(s.client)
	if err != nil {
		log.Println("Failed to open Kafka consumer:", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Println("Failed to close Kafka consumer cleanly:", err)
		}
	}()

	pc, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		log.Println("Failed to open Kafka partition consumer:", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	streamMessages(w, pc)

	if err := pc.Close(); err != nil {
		log.Println("Failed to close Kafka partition consumer cleanly:", err)
	}
}

func streamMessages(w http.ResponseWriter, pc sarama.PartitionConsumer) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	for msg := range pc.Messages() {
		sseEvent := kafkaMessageToSSEMessage(msg)
		if _, err := w.Write([]byte(sseEvent)); err != nil {
			// If we fail to write the message to the response for some reason,
			// most likely the client is gone, so we just stop the stream.
			break
		}

		// Flush data to the client, to ensure it receives the full message
		w.(http.Flusher).Flush()
	}
}

func (s *SSEServer) Run(addr string) error {
	httpServer := &http.Server{
		Addr:    addr,
		Handler: s.Handler(),
	}

	log.Printf("Listening for requests on %s...\n", addr)
	return httpServer.ListenAndServe()
}

func (s *SSEServer) Close() error {
	return s.client.Close()
}

FROM golang:1.6

RUN mkdir -p /go/src/github.com/Shopify/sarama

COPY . /go/src/github.com/Shopify/sarama

RUN cd /go/src/github.com/Shopify/sarama && \
	go-wrapper download

# Build our tools
RUN go build /go/src/github.com/Shopify/sarama/tools/kafka-console-producer/kafka-console-producer.go && \
	mv kafka-console-producer /go/bin/

RUN cd /go/src/github.com/Shopify/sarama && \
	go build tools/kafka-console-consumer/kafka-console-consumer.go && \
	mv kafka-console-consumer /go/bin/

RUN cd /go/src/github.com/Shopify/sarama && \
	go build tools/kafka-console-partitionconsumer/kafka-console-partitionconsumer.go && \
	mv kafka-console-partitionconsumer /go/bin/

WORKDIR /go/bin
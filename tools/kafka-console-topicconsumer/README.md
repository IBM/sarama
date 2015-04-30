# kafka-console-topicconsumer

A simple command line tool to consume all partitions a topic and print the messages
on the standard output.

### Installation

    go install github.com/Shopify/sarama/tools/kafka-console-topicconsumer

### Usage

    # Minimum invocation
    kafka-console-topicconsumer -topic=test -brokers=kafka1:9092

    # It will pick up a KAFKA_PEERS environment variable
    export KAFKA_PEERS=kafka1:9092,kafka2:9092,kafka3:9092
    kafka-console-topicconsumer -topic=test

    # You can specify the offset you want to start at. It can be either
    # `oldest`, `newest`. The default is `newest`.
    kafka-console-topicconsumer -topic=test -offset=oldest
    kafka-console-topicconsumer -topic=test -offset=newest

    # Display all command line options
    kafka-console-topicconsumer -help

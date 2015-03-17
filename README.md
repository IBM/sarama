sarama
======

[![GoDoc](https://godoc.org/github.com/Shopify/sarama?status.png)](https://godoc.org/github.com/Shopify/sarama)
[![Build Status](https://travis-ci.org/Shopify/sarama.svg?branch=master)](https://travis-ci.org/Shopify/sarama)

Sarama is an MIT-licensed Go client library for Apache Kafka 0.8 (and later).

Documentation is available via godoc at https://godoc.org/github.com/Shopify/sarama

There is a google group for Kafka client users and authors at https://groups.google.com/forum/#!forum/kafka-clients

Sarama provides a "2 releases + 2 months" compatibility guarantee: we support the two latest releases of Kafka
and Go, and we provide a two month grace period for older releases. This means we currently officially
support Go 1.3 and 1.4, and Kafka 0.8.1 and 0.8.2.

Sarama follows semantic versioning and provides API stability via the gopkg.in
service. You can import a version with a guaranteed stable API via
http://gopkg.in/Shopify/sarama.v1. A changelog is available
[here](CHANGELOG.md).

Other related links:
* [Kafka Project Home](https://kafka.apache.org/)
* [Kafka Protocol Specification](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)

sarama
======

[![GoDoc](https://godoc.org/github.com/Shopify/sarama?status.png)](https://godoc.org/github.com/Shopify/sarama)
[![Build Status](https://travis-ci.org/Shopify/sarama.svg?branch=master)](https://travis-ci.org/Shopify/sarama)

Sarama is an MIT-licensed Go client library for [Apache Kafka](https://kafka.apache.org/) version 0.8 (and later).

### Getting started

- API documentation and examples are available via [godoc](https://godoc.org/github.com/Shopify/sarama).
- Mocks for testing are available in the [mocks](./mocks) subpackage.
- The [examples](./examples) directory contains more elaborate example applications.
- The [tools](./tools) directory contains command line tools that can be useful for testing, diagnostics, and instrumentation.
- There is a google group for Kafka client users and authors at https://groups.google.com/forum/#!forum/kafka-clients

### Compatibility and API stability

Sarama provides a "2 releases + 2 months" compatibility guarantee: we support
the two latest stable releases of Kafka and Go, and we provide a two month
grace period for older releases. This means we currently officially support
Go 1.4 and 1.5, and Kafka 0.8.1 and 0.8.2, although older releases are still
likely to work.

Sarama follows semantic versioning and provides API stability via the gopkg.in service.
You can import a version with a guaranteed stable API via http://gopkg.in/Shopify/sarama.v1.
A changelog is available [here](CHANGELOG.md).

### Other Links

* [Sarama wiki](https://github.com/Shopify/sarama/wiki) to get started contributing to sarama itself.
* [Kafka Project Home](https://kafka.apache.org/)
* [Kafka Protocol Specification](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)

# Sarama examples

This folder contains example applications to demonstrate the use of Sarama. For code snippet examples on how to use the different types in Sarama, see [Sarama's API documentation on pkg.go.dev](https://pkg.go.dev/github.com/Shopify/sarama)

In these examples, we use `github.com/Shopify/sarama` as import path. We do this to ensure all the examples are up to date with the latest changes in Sarama. For your own applications, you may want to use `gopkg.in/Shopify/sarama.v1` to lock into a stable API version.

#### HTTP server

[http_server](./http_server) is a simple HTTP server uses both the sync producer to produce data as part of the request handling cycle, as well as the async producer to maintain an access log. It also uses the [mocks subpackage](https://pkg.go.dev/github.com/Shopify/sarama/mocks) to test both.

#### Interceptors

Basic example to use a producer interceptor that produces [OpenTelemetry](https://github.com/open-telemetry/opentelemetry-go/) spans and add some headers for each intercepted message.

#### Transactional Producer

[txn_producer](./txn_producer) Basic example to use a transactional producer that produce on some topic within a Kafka transaction. To ensure transactional-id uniqueness it implement some ***ProducerProvider*** that build a producer appending an integer that grow when producer is created.

#### Exacly-once transactional paradigm

[exactly_once](./exactly_once) Basic example to use a transactional producer that produce consumed message from some topics within a Kafka transaction. To ensure transactional-id uniqueness it implement some ***ProducerProvider*** that build a producer using current message topic-partition.

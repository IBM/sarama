# Sarama examples

This folder contains example applications to demonstrate the use of Sarama. For code snippet examples on how to use the different types in Sarama, see [Sarams's API documentation on godoc.org](https://godoc.org/github.com/Shopify/sarama)

#### HTTP server

[http_server](./http_server) is a simple HTTP server uses both the sync producer to produce data as part of the request handling cycle, as well as the async producer to maintain an access log. It also uses the [mocks subpackage](https://godoc.org/github.com/Shopify/sarama/mocks) to test both.

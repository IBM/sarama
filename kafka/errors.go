package kafka

import "errors"

// Error returned when the client has run out of brokers to talk to (none of them are responding).
var OutOfBrokers = errors.New("kafka: Client has run out of available brokers to talk to. Is your cluster reachable?")

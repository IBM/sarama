package kafka

import "errors"

// OutOfBrokers is the error returned when the client has run out of brokers to talk to because all of them errored
// or otherwise failed to respond.
var OutOfBrokers = errors.New("kafka: Client has run out of available brokers to talk to. Is your cluster reachable?")

// NoSuchTopic is the error returned when the supplied topic is rejected by the Kafka servers.
var NoSuchTopic = errors.New("kafka: Topic not recognized by brokers.")

// IncompleteResponse is the error returned when the server returns a syntactically valid response, but it does
// not contain the expected information.
var IncompleteResponse = errors.New("kafka: Response did not contain all the expected topic/partition blocks.")

/*
Package sarama provides client libraries for the Kafka 0.8 protocol.

Package sarama is a dummy package, you almost certainly want sarama/kafka instead, which contains the high-level `userspace` API.

The sarama/kafka package is built on sarama/protocol, which contains the lower-level API that gives you control over which requests get sent to which brokers.

The sarama/protocol package is build on sarama/encoding, which implements the Kafka encoding rules for strings and other data structures.

The sarama/mock package exposes some very basic helper functions for mocking a Kafka broker for testing purposes.
*/
package sarama

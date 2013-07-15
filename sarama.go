/*
Package kafka (AKA sarama.go) provides client libraries for the Kafka 0.8 protocol.

It provides a high-level API to make common tasks easy, as well as a low-level API for precise control
over message batching etc. The high-level API consists of Client, Producer, Consumer, Encoder.

If you need more control, you can connect to Kafka brokers directly using the Broker object,
then send requests and receive responses using functions on the broker itself.
*/
package kafka

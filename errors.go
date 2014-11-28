package sarama

import (
	"errors"
	"fmt"
)

// OutOfBrokers is the error returned when the client has run out of brokers to talk to because all of them errored
// or otherwise failed to respond.
var OutOfBrokers = errors.New("kafka: Client has run out of available brokers to talk to. Is your cluster reachable?")

// ClosedClient is the error returned when a method is called on a client that has been closed.
var ClosedClient = errors.New("kafka: Tried to use a client that was closed.")

// IncompleteResponse is the error returned when the server returns a syntactically valid response, but it does
// not contain the expected information.
var IncompleteResponse = errors.New("kafka: Response did not contain all the expected topic/partition blocks.")

// InvalidPartition is the error returned when a partitioner returns an invalid partition index
// (meaning one outside of the range [0...numPartitions-1]).
var InvalidPartition = errors.New("kafka: Partitioner returned an invalid partition index.")

// AlreadyConnected is the error returned when calling Open() on a Broker that is already connected.
var AlreadyConnected = errors.New("kafka: broker: already connected")

// NotConnected is the error returned when trying to send or call Close() on a Broker that is not connected.
var NotConnected = errors.New("kafka: broker: not connected")

// EncodingError is returned from a failure while encoding a Kafka packet. This can happen, for example,
// if you try to encode a string over 2^15 characters in length, since Kafka's encoding rules do not permit that.
var EncodingError = errors.New("kafka: Error while encoding packet.")

// InsufficientData is returned when decoding and the packet is truncated. This can be expected
// when requesting messages, since as an optimization the server is allowed to return a partial message at the end
// of the message set.
var InsufficientData = errors.New("kafka: Insufficient data to decode packet, more bytes expected.")

// ShuttingDown is returned when a producer receives a message during shutdown.
var ShuttingDown = errors.New("kafka: Message received by producer in process of shutting down.")

// DecodingError is returned when there was an error (other than truncated data) decoding the Kafka broker's response.
// This can be a bad CRC or length field, or any other invalid value.
type DecodingError struct {
	Info string
}

func (err DecodingError) Error() string {
	return fmt.Sprintf("kafka: Error while decoding packet: %s", err.Info)
}

// MessageTooLarge is returned when the next message to consume is larger than the configured MaxFetchSize
var MessageTooLarge = errors.New("kafka: Message is larger than MaxFetchSize")

// ConfigurationError is the type of error returned from NewClient, NewProducer or NewConsumer when the specified
// configuration is invalid.
type ConfigurationError string

func (err ConfigurationError) Error() string {
	return "kafka: Invalid Configuration: " + string(err)
}

// KError is the type of error that can be returned directly by the Kafka broker.
// See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes
type KError int16

// Numeric error codes returned by the Kafka server.
const (
	NoError                         KError = 0
	Unknown                         KError = -1
	OffsetOutOfRange                KError = 1
	InvalidMessage                  KError = 2
	UnknownTopicOrPartition         KError = 3
	InvalidMessageSize              KError = 4
	LeaderNotAvailable              KError = 5
	NotLeaderForPartition           KError = 6
	RequestTimedOut                 KError = 7
	BrokerNotAvailable              KError = 8
	ReplicaNotAvailable             KError = 9
	MessageSizeTooLarge             KError = 10
	StaleControllerEpochCode        KError = 11
	OffsetMetadataTooLarge          KError = 12
	OffsetsLoadInProgress           KError = 14
	ConsumerCoordinatorNotAvailable KError = 15
	NotCoordinatorForConsumer       KError = 16
)

func (err KError) Error() string {
	// Error messages stolen/adapted from
	// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
	switch err {
	case NoError:
		return "kafka server: Not an error, why are you printing me?"
	case Unknown:
		return "kafka server: Unexpected (unknown?) server error."
	case OffsetOutOfRange:
		return "kafka server: The requested offset is outside the range of offsets maintained by the server for the given topic/partition."
	case InvalidMessage:
		return "kafka server: Message contents does not match its CRC."
	case UnknownTopicOrPartition:
		return "kafka server: Request was for a topic or partition that does not exist on this broker."
	case InvalidMessageSize:
		return "kafka server: The message has a negative size."
	case LeaderNotAvailable:
		return "kafka server: In the middle of a leadership election, there is currently no leader for this partition and hence it is unavailable for writes."
	case NotLeaderForPartition:
		return "kafka server: Tried to send a message to a replica that is not the leader for some partition. Your metadata is out of date."
	case RequestTimedOut:
		return "kafka server: Request exceeded the user-specified time limit in the request."
	case BrokerNotAvailable:
		return "kafka server: Broker not available. Not a client facing error, we should never receive this!!!"
	case ReplicaNotAvailable:
		return "kafka server: Replica infomation not available, one or more brokers are down."
	case MessageSizeTooLarge:
		return "kafka server: Message was too large, server rejected it to avoid allocation error."
	case StaleControllerEpochCode:
		return "kafka server: StaleControllerEpochCode (internal error code for broker-to-broker communication)."
	case OffsetMetadataTooLarge:
		return "kafka server: Specified a string larger than the configured maximum for offset metadata."
	case OffsetsLoadInProgress:
		return "kafka server: The broker is still loading offsets after a leader change for that offset's topic partition."
	case ConsumerCoordinatorNotAvailable:
		return "kafka server: Offset's topic has not yet been created."
	case NotCoordinatorForConsumer:
		return "kafka server: Request was for a consumer group that is not coordinated by this broker."
	}

	return fmt.Sprintf("Unknown error, how did this happen? Error code = %d", err)
}

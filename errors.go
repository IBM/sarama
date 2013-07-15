package kafka

import "errors"

// The various errors that can be returned by the Kafka server.
// See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes
type KError int16

const (
	NO_ERROR                    KError = 0
	UNKNOWN                     KError = -1
	OFFSET_OUT_OF_RANGE         KError = 1
	INVALID_MESSAGE             KError = 2
	UNKNOWN_TOPIC_OR_PARTITION  KError = 3
	INVALID_MESSAGE_SIZE        KError = 4
	LEADER_NOT_AVAILABLE        KError = 5
	NOT_LEADER_FOR_PARTITION    KError = 6
	REQUEST_TIMED_OUT           KError = 7
	BROKER_NOT_AVAILABLE        KError = 8
	REPLICA_NOT_AVAILABLE       KError = 9
	MESSAGE_SIZE_TOO_LARGE      KError = 10
	STALE_CONTROLLER_EPOCH_CODE KError = 11
	OFFSET_METADATA_TOO_LARGE   KError = 12
)

func (err KError) Error() string {
	// Error messages stolen/adapted from
	// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
	switch err {
	case NO_ERROR:
		return "kafka server: Not an error, why are you printing me?"
	case UNKNOWN:
		return "kafka server: Unexpected (unknown?) server error."
	case OFFSET_OUT_OF_RANGE:
		return "kafka server: The requested offset is outside the range of offsets maintained by the server for the given topic/partition."
	case INVALID_MESSAGE:
		return "kafka server: Message contents does not match its CRC."
	case UNKNOWN_TOPIC_OR_PARTITION:
		return "kafka server: Request was for a topic or partition that does not exist on this broker."
	case INVALID_MESSAGE_SIZE:
		return "kafka server: The message has a negative size."
	case LEADER_NOT_AVAILABLE:
		return "kafka server: In the middle of a leadership election, there is currently no leader for this partition and hence it is unavailable for writes."
	case NOT_LEADER_FOR_PARTITION:
		return "kafka server: Tried to send a message to a replica that is not the leader for some partition. Your metadata is out of date."
	case REQUEST_TIMED_OUT:
		return "kafka server: Request exceeded the user-specified time limit in the request."
	case BROKER_NOT_AVAILABLE:
		return "kafka server: Broker not available. Not a client facing error, we should never receive this!!!"
	case REPLICA_NOT_AVAILABLE:
		return "kafka server: Replica not available. What is the difference between this and LeaderNotAvailable?"
	case MESSAGE_SIZE_TOO_LARGE:
		return "kafka server: Message was too large, server rejected it to avoid allocation error."
	case STALE_CONTROLLER_EPOCH_CODE:
		return "kafka server: Stale controller epoch code. ???"
	case OFFSET_METADATA_TOO_LARGE:
		return "kafka server: Specified a string larger than the configured maximum for offset metadata."
	default:
		return "Unknown error, how did this happen?"
	}
}

// Error returned when the client has run out of brokers to talk to (none of them are responding).
var OutOfBrokers = errors.New("kafka: Client has run out of available brokers to talk to. Is your cluster reachable?")

// Error returned when calling Connect() on a Broker that is already connected.
var AlreadyConnected = errors.New("kafka: broker: already connected")

// Error returned when trying to send or call Close() on a Broker that is not connected.
var NotConnected = errors.New("kafka: broker: not connected")

// Returned when there was an error encoding a Kafka packet. This can happen, for example,
// if you try to encode a string over 2^15 characters in length, since Kafka's encoding rules do
// not permit that.
type EncodingError string

func (err EncodingError) Error() string {
	return "kafka: Could not encode packet. " + string(err)
}

// Returned when there was an error decoding the Kafka server's response. Usually means that you've
// connected to the wrong address.
type DecodingError string

func (err DecodingError) Error() string {
	return "kafka: Could not decode packet. " + string(err)
}

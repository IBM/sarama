package kafka

import "errors"

// AlreadyConnected is the error returned when calling Connect() on a Broker that is already connected.
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

// DecodingError is returned when there was an error (other than truncated data) decoding the Kafka broker's response.
// This can be a bad CRC or length field, or any other invalid value.
var DecodingError = errors.New("kafka: Error while decoding packet.")

// KError is the type of error that can be returned directly by the Kafka broker.
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
